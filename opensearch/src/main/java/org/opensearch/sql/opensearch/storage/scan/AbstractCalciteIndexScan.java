/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NumberUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/** An abstract relational operator representing a scan of an OpenSearchIndex type. */
@Getter
public abstract class AbstractCalciteIndexScan extends TableScan {
  private static final Logger LOG = LogManager.getLogger(AbstractCalciteIndexScan.class);
  public final OpenSearchIndex osIndex;
  // The schema of this scan operator, it's initialized with the row type of the table, but may be
  // changed by push down operations.
  protected final RelDataType schema;
  // This context maintains all the push down actions, which will be applied to the requestBuilder
  // when it begins to scan data from OpenSearch.
  // Because OpenSearchRequestBuilder doesn't support deep copy while we want to keep the
  // requestBuilder independent among different plans produced in the optimization process,
  // so we cannot apply these actions right away.
  protected final PushDownContext pushDownContext;

  protected AbstractCalciteIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext) {
    super(cluster, traitSet, hints, table);
    this.osIndex = requireNonNull(osIndex, "OpenSearch index");
    this.schema = schema;
    this.pushDownContext = pushDownContext;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.schema;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    OpenSearchRequestBuilder requestBuilder = osIndex.createRequestBuilder();
    pushDownContext.forEach(action -> action.apply(requestBuilder));
    String explainString = pushDownContext + ", " + requestBuilder;
    return super.explainTerms(pw)
        .itemIf("PushDownContext", explainString, !pushDownContext.isEmpty());
  }

  protected Integer getQuerySizeLimit() {
    return osIndex.getSettings().getSettingValue(Key.QUERY_SIZE_LIMIT);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    /*
     The impact factor to estimate the row count after push down an operator.

     <p>It will be multiplied to the original estimated row count of the operator, and it's set to
     less than 1 by default to make the result always less than the row count of operator without
     push down. As a result, the optimizer will prefer the plan with push down.
    */
    double estimateRowCountFactor =
        osIndex.getSettings().getSettingValue(CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR);
    return pushDownContext.stream()
        .reduce(
            osIndex.getMaxResultWindow().doubleValue(),
            (rowCount, action) ->
                switch (action.type) {
                      case AGGREGATION -> mq.getRowCount((RelNode) action.digest)
                          * getAggMultiplier(action);
                      case PROJECT, SORT -> rowCount;
                        // Refer the org.apache.calcite.rel.core.Aggregate.estimateRowCount
                      case COLLAPSE -> rowCount * (1.0 - Math.pow(.5, 1));
                      case FILTER -> NumberUtil.multiply(
                          rowCount, RelMdUtil.guessSelectivity((RexNode) action.digest));
                      case SCRIPT -> NumberUtil.multiply(
                              rowCount, RelMdUtil.guessSelectivity((RexNode) action.digest))
                          * 1.1;
                      case LIMIT -> Math.min(rowCount, ((LimitDigest) action.digest).limit());
                    }
                    * estimateRowCountFactor,
            (a, b) -> null);
  }

  /** See source in {@link org.apache.calcite.rel.core.Aggregate::computeSelfCost} */
  private static float getAggMultiplier(PushDownAction action) {
    // START CALCITE
    List<AggregateCall> aggCalls = ((Aggregate) action.digest).getAggCallList();
    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.getAggregation().getName().equals("SUM")) {
        // Pretend that SUM costs a little bit more than $SUM0,
        // to make things deterministic.
        multiplier += 0.0125f;
      }
    }
    // END CALCITE

    // For script aggregation, we need to multiply the multiplier by 2.2 to make up the cost. As we
    // prefer to have non-script agg push down after optimized by {@link PPLAggregateConvertRule}
    if (((AggPushDownAction) action.action).isScriptPushed) {
      multiplier *= 2.2f;
    }
    return multiplier;
  }

  // TODO: should we consider equivalent among PushDownContexts with different push down sequence?
  public static class PushDownContext extends ArrayDeque<PushDownAction> {

    @Getter private boolean isAggregatePushed = false;
    @Getter private AggPushDownAction aggPushDownAction;
    @Getter private boolean isLimitPushed = false;
    @Getter private boolean isProjectPushed = false;
    @Getter private int startFrom = 0;

    @Override
    public PushDownContext clone() {
      return (PushDownContext) super.clone();
    }

    @Override
    public boolean add(PushDownAction pushDownAction) {
      if (pushDownAction.type == PushDownType.AGGREGATION) {
        isAggregatePushed = true;
        this.aggPushDownAction = (AggPushDownAction) pushDownAction.action;
      }
      if (pushDownAction.type == PushDownType.LIMIT) {
        isLimitPushed = true;
        startFrom += ((LimitDigest) pushDownAction.digest).offset();
      }
      if (pushDownAction.type == PushDownType.PROJECT) {
        isProjectPushed = true;
      }
      return super.add(pushDownAction);
    }

    public boolean containsDigest(Object digest) {
      return this.stream().anyMatch(action -> action.digest.equals(digest));
    }
  }

  protected abstract AbstractCalciteIndexScan buildScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex,
      RelDataType schema,
      PushDownContext pushDownContext);

  private List<String> getCollationNames(List<RelFieldCollation> collations) {
    return collations.stream()
        .map(collation -> getRowType().getFieldNames().get(collation.getFieldIndex()))
        .toList();
  }

  /**
   * Check if the sort by collations contains any aggregators that are pushed down. E.g. In `stats
   * avg(age) as avg_age by state | sort avg_age`, the sort clause has `avg_age` which is an
   * aggregator. The function will return true in this case.
   *
   * @param collations List of collation names to check against aggregators.
   * @return True if any collation name matches an aggregator output, false otherwise.
   */
  private boolean hasAggregatorInSortBy(List<String> collations) {
    Stream<LogicalAggregate> aggregates =
        pushDownContext.stream()
            .filter(action -> action.type() == PushDownType.AGGREGATION)
            .map(action -> ((LogicalAggregate) action.digest()));
    return aggregates
        .map(aggregate -> isAnyCollationNameInAggregateOutput(aggregate, collations))
        .reduce(false, Boolean::logicalOr);
  }

  private static boolean isAnyCollationNameInAggregateOutput(
      LogicalAggregate aggregate, List<String> collations) {
    List<String> fieldNames = aggregate.getRowType().getFieldNames();
    // The output fields of the aggregate are in the format of
    // [...grouping fields, ...aggregator fields], so we set an offset to skip
    // the grouping fields.
    int groupOffset = aggregate.getGroupSet().cardinality();
    List<String> fieldsWithoutGrouping = fieldNames.subList(groupOffset, fieldNames.size());
    return collations.stream()
        .map(fieldsWithoutGrouping::contains)
        .reduce(false, Boolean::logicalOr);
  }

  /**
   * Create a new {@link PushDownContext} without the collation action.
   *
   * @param pushDownContext The original push-down context.
   * @return A new push-down context without the collation action.
   */
  protected PushDownContext cloneWithoutSort(PushDownContext pushDownContext) {
    PushDownContext newContext = new PushDownContext();
    for (PushDownAction action : pushDownContext) {
      if (action.type() != PushDownType.SORT) {
        newContext.add(action);
      }
    }
    return newContext;
  }

  /**
   * The sort pushdown is not only applied in logical plan side, but also should be applied in
   * physical plan side. Because we could push down the {@link EnumerableSort} of {@link
   * EnumerableMergeJoin} to OpenSearch.
   */
  public AbstractCalciteIndexScan pushDownSort(List<RelFieldCollation> collations) {
    try {
      List<String> collationNames = getCollationNames(collations);
      if (getPushDownContext().isAggregatePushed() && hasAggregatorInSortBy(collationNames)) {
        // If aggregation is pushed down, we cannot push down sorts where its by fields contain
        // aggregators.
        return null;
      }

      // Propagate the sort to the new scan
      RelTraitSet traitsWithCollations = getTraitSet().plus(RelCollations.of(collations));
      AbstractCalciteIndexScan newScan =
          buildScan(
              getCluster(),
              traitsWithCollations,
              hints,
              table,
              osIndex,
              getRowType(),
              // Existing collations are overridden (discarded) by the new collations,
              cloneWithoutSort(pushDownContext));

      AbstractAction action;
      Object digest;
      if (pushDownContext.isAggregatePushed) {
        // Push down the sort into the aggregation bucket
        this.pushDownContext.aggPushDownAction.pushDownSortIntoAggBucket(
            collations, getRowType().getFieldNames());
        action = requestBuilder -> {};
        digest = collations;
      } else {
        List<SortBuilder<?>> builders = new ArrayList<>();
        for (RelFieldCollation collation : collations) {
          int index = collation.getFieldIndex();
          String fieldName = this.getRowType().getFieldNames().get(index);
          Direction direction = collation.getDirection();
          NullDirection nullDirection = collation.nullDirection;
          // Default sort order is ASCENDING
          SortOrder order = Direction.DESCENDING.equals(direction) ? SortOrder.DESC : SortOrder.ASC;
          // TODO: support script sort and distance sort
          SortBuilder<?> sortBuilder;
          if (ScoreSortBuilder.NAME.equals(fieldName)) {
            sortBuilder = SortBuilders.scoreSort();
          } else {
            String missing =
                switch (nullDirection) {
                  case FIRST -> "_first";
                  case LAST -> "_last";
                  default -> null;
                };
            // Keyword field is optimized for sorting in OpenSearch
            ExprType fieldType = osIndex.getFieldTypes().get(fieldName);
            String field = OpenSearchTextType.toKeywordSubField(fieldName, fieldType);
            sortBuilder = SortBuilders.fieldSort(field).missing(missing);
          }
          builders.add(sortBuilder.order(order));
        }
        action = requestBuilder -> requestBuilder.pushDownSort(builders);
        digest = builders.toString();
      }
      newScan.pushDownContext.add(PushDownAction.of(PushDownType.SORT, digest, action));
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort {}", getCollationNames(collations), e);
      }
    }
    return null;
  }

  protected enum PushDownType {
    FILTER,
    PROJECT,
    AGGREGATION,
    SORT,
    LIMIT,
    SCRIPT,
    COLLAPSE
    // HIGHLIGHT,
    // NESTED
  }

  /**
   * Represents a push down action that can be applied to an OpenSearchRequestBuilder.
   *
   * @param type PushDownType enum
   * @param digest the digest of the pushed down operator
   * @param action the lambda action to apply on the OpenSearchRequestBuilder
   */
  public record PushDownAction(PushDownType type, Object digest, AbstractAction action) {
    static PushDownAction of(PushDownType type, Object digest, AbstractAction action) {
      return new PushDownAction(type, digest, action);
    }

    public String toString() {
      return type + "->" + digest;
    }

    public void apply(OpenSearchRequestBuilder requestBuilder) {
      action.apply(requestBuilder);
    }
  }

  public interface AbstractAction {
    void apply(OpenSearchRequestBuilder requestBuilder);
  }

  public record LimitDigest(int limit, int offset) {
    @Override
    public String toString() {
      return offset == 0 ? String.valueOf(limit) : "[" + limit + " from " + offset + "]";
    }
  }

  // TODO: shall we do deep copy for this action since it's mutable?
  public static class AggPushDownAction implements AbstractAction {

    private Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder;
    private final Map<String, OpenSearchDataType> extendedTypeMapping;
    @Getter private final boolean isScriptPushed;
    // Record the output field names of all buckets as the sequence of buckets
    private List<String> bucketNames;

    public AggPushDownAction(
        Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder,
        Map<String, OpenSearchDataType> extendedTypeMapping,
        List<String> bucketNames) {
      this.aggregationBuilder = aggregationBuilder;
      this.extendedTypeMapping = extendedTypeMapping;
      this.isScriptPushed =
          aggregationBuilder.getLeft().stream().anyMatch(this::isScriptAggBuilder);
      this.bucketNames = bucketNames;
    }

    private boolean isScriptAggBuilder(AggregationBuilder aggBuilder) {
      return aggBuilder instanceof ValuesSourceAggregationBuilder<?> valueSourceAgg
          && valueSourceAgg.script() != null;
    }

    @Override
    public void apply(OpenSearchRequestBuilder requestBuilder) {
      requestBuilder.pushDownAggregation(aggregationBuilder);
      requestBuilder.pushTypeMapping(extendedTypeMapping);
    }

    public void pushDownSortIntoAggBucket(
        List<RelFieldCollation> collations, List<String> fieldNames) {
      // aggregationBuilder.getLeft() could be empty when count agg optimization works
      if (aggregationBuilder.getLeft().isEmpty()) return;
      AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
      List<String> selected = new ArrayList<>(collations.size());
      if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
        // It will always use a single CompositeAggregationBuilder for the aggregation with GroupBy
        // See {@link AggregateAnalyzer}
        List<CompositeValuesSourceBuilder<?>> buckets = compositeAggBuilder.sources();
        List<CompositeValuesSourceBuilder<?>> newBuckets = new ArrayList<>(buckets.size());
        List<String> newBucketNames = new ArrayList<>(buckets.size());
        // Have to put the collation required buckets first, then the rest of buckets.
        collations.forEach(
            collation -> {
              /*
               Must find the bucket by field name because:
                 1. The sequence of buckets may have changed after sort push-down.
                 2. The schema of scan operator may be inconsistent with the sequence of buckets
                 after project push-down.
              */
              String bucketName = fieldNames.get(collation.getFieldIndex());
              CompositeValuesSourceBuilder<?> bucket = buckets.get(bucketNames.indexOf(bucketName));
              Direction direction = collation.getDirection();
              NullDirection nullDirection = collation.nullDirection;
              SortOrder order =
                  Direction.DESCENDING.equals(direction) ? SortOrder.DESC : SortOrder.ASC;
              if (bucket.missingBucket()) {
                MissingOrder missingOrder =
                    switch (nullDirection) {
                      case FIRST -> MissingOrder.FIRST;
                      case LAST -> MissingOrder.LAST;
                      default -> MissingOrder.DEFAULT;
                    };
                bucket.missingOrder(missingOrder);
              }
              newBuckets.add(bucket.order(order));
              newBucketNames.add(bucketName);
              selected.add(bucketName);
            });
        IntStream.range(0, buckets.size())
            .mapToObj(fieldNames::get)
            .filter(name -> !selected.contains(name))
            .forEach(
                name -> {
                  newBuckets.add(buckets.get(bucketNames.indexOf(name)));
                  newBucketNames.add(name);
                });
        Builder newAggBuilder = new Builder();
        compositeAggBuilder.getSubAggregations().forEach(newAggBuilder::addAggregator);
        aggregationBuilder =
            Pair.of(
                Collections.singletonList(
                    AggregationBuilders.composite("composite_buckets", newBuckets)
                        .subAggregations(newAggBuilder)
                        .size(compositeAggBuilder.size())),
                aggregationBuilder.getRight());
        bucketNames = newBucketNames;
      }
      if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
        termsAggBuilder.order(
            BucketOrder.key(!collations.getFirst().getDirection().isDescending()));
      }
      // TODO for MultiTermsAggregationBuilder
    }

    /**
     * Check if the limit can be pushed down into aggregation bucket when the limit size is less
     * than bucket number.
     */
    public boolean pushDownLimitIntoBucketSize(Integer size) {
      // aggregationBuilder.getLeft() could be empty when count agg optimization works
      if (aggregationBuilder.getLeft().isEmpty()) return false;
      AggregationBuilder builder = aggregationBuilder.getLeft().getFirst();
      if (builder instanceof CompositeAggregationBuilder compositeAggBuilder) {
        if (size < compositeAggBuilder.size()) {
          compositeAggBuilder.size(size);
          return true;
        } else {
          return false;
        }
      }
      if (builder instanceof TermsAggregationBuilder termsAggBuilder) {
        if (size < termsAggBuilder.size()) {
          termsAggBuilder.size(size);
          return true;
        } else {
          return false;
        }
      }
      if (builder instanceof MultiTermsAggregationBuilder multiTermsAggBuilder) {
        if (size < multiTermsAggBuilder.size()) {
          multiTermsAggBuilder.size(size);
          return true;
        } else {
          return false;
        }
      }
      // now we only have Composite, Terms and MultiTerms bucket aggregations,
      // add code here when we could support more in the future.
      if (builder instanceof ValuesSourceAggregationBuilder.LeafOnly<?, ?>) {
        // Note: all metric aggregations will be treated as pushed since it generates only one row.
        return true;
      }
      throw new OpenSearchRequestBuilder.PushDownUnSupportedException(
          "Unknown aggregation builder " + builder.getClass().getSimpleName());
    }
  }
}
