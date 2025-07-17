/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NumberUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
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
                      case AGGREGATION -> mq.getRowCount((RelNode) action.digest);
                      case PROJECT, SORT -> rowCount;
                      case FILTER -> NumberUtil.multiply(
                          rowCount, RelMdUtil.guessSelectivity((RexNode) action.digest));
                      case LIMIT -> Math.min(rowCount, (Integer) action.digest);
                    }
                    * estimateRowCountFactor,
            (a, b) -> null);
  }

  // TODO: should we consider equivalent among PushDownContexts with different push down sequence?
  public static class PushDownContext extends ArrayDeque<PushDownAction> {

    private boolean isAggregatePushed = false;
    @Getter private boolean isLimitPushed = false;

    @Override
    public PushDownContext clone() {
      return (PushDownContext) super.clone();
    }

    @Override
    public boolean add(PushDownAction pushDownAction) {
      if (pushDownAction.type == PushDownType.AGGREGATION) {
        isAggregatePushed = true;
      }
      if (pushDownAction.type == PushDownType.LIMIT) {
        isLimitPushed = true;
      }
      return super.add(pushDownAction);
    }

    public boolean isAggregatePushed() {
      if (isAggregatePushed) return true;
      isAggregatePushed = !isEmpty() && super.peekLast().type == PushDownType.AGGREGATION;
      return isAggregatePushed;
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

      List<SortBuilder<?>> builders = new ArrayList<>();
      for (RelFieldCollation collation : collations) {
        int index = collation.getFieldIndex();
        String fieldName = this.getRowType().getFieldNames().get(index);
        RelFieldCollation.Direction direction = collation.getDirection();
        RelFieldCollation.NullDirection nullDirection = collation.nullDirection;
        // Default sort order is ASCENDING
        SortOrder order =
            RelFieldCollation.Direction.DESCENDING.equals(direction)
                ? SortOrder.DESC
                : SortOrder.ASC;
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
      newScan.pushDownContext.add(
          PushDownAction.of(
              PushDownType.SORT,
              builders.toString(),
              requestBuilder -> requestBuilder.pushDownSort(builders)));
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort {}", getCollationNames(collations), e);
      } else {
        LOG.info("Cannot pushdown the sort {}, ", getCollationNames(collations));
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
    // HIGHLIGHT,
    // NESTED
  }

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
}
