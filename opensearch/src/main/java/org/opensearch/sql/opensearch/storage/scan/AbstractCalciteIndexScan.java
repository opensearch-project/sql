/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableMergeJoin;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
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
import org.apache.calcite.util.NumberUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
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
    String explainString = pushDownContext + ", " + pushDownContext.getRequestBuilder();
    return super.explainTerms(pw)
        .itemIf("PushDownContext", explainString, !pushDownContext.isEmpty());
  }

  protected Integer getQuerySizeLimit() {
    return osIndex.getSettings().getSettingValue(Key.QUERY_SIZE_LIMIT);
  }

  /**
   * Compute the final row count of the scan operator with the given push down operations.
   *
   * <p>The calculation logic tries to follow the same logic in Calcite.
   */
  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return pushDownContext.stream()
        .reduce(
            osIndex.getMaxResultWindow().doubleValue(),
            (rowCount, operation) ->
                switch (operation.type()) {
                  case AGGREGATION -> mq.getRowCount((RelNode) operation.digest());
                  case PROJECT, SORT -> rowCount;
                    // Refer the org.apache.calcite.rel.metadata.RelMdRowCount
                  case COLLAPSE -> rowCount / 10;
                  case FILTER, SCRIPT -> NumberUtil.multiply(
                      rowCount,
                      RelMdUtil.guessSelectivity(((FilterDigest) operation.digest()).condition()));
                  case LIMIT -> Math.min(rowCount, ((LimitDigest) operation.digest()).limit());
                },
            (a, b) -> null);
  }

  /**
   * Compute the cost of the scan operator with the given push down operations.
   *
   * <p>We compute the final cost of the scan operator by accumulating the cost of each push down
   * operation including aggregation, collapse, sort and script filter, and plus an external cost.
   * The calculation logic tries to follow the same logic in Calcite.
   *
   * <p>While the left operations like project, filter, limit will be ignored in the accumulation
   * process. But they will also affect the cost of cost-counted operations after them and the final
   * external cost, which is calculated by `rows count * fields count`.
   *
   * <p>In the end, we still need to multiply the total cost by a factor to make the cost cheaper
   * than non-pushdown operators.
   */
  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double dRows = osIndex.getMaxResultWindow().doubleValue(), dCpu = 0.0d;
    for (PushDownOperation operation : pushDownContext) {
      switch (operation.type()) {
        case AGGREGATION -> {
          dRows = mq.getRowCount((RelNode) operation.digest());
          dCpu += dRows * getAggMultiplier(operation);
        }
          // Ignored Project in cost accumulation, but it will affect the external cost
        case PROJECT -> {}
        case SORT -> dCpu += dRows;
          // Refer the org.apache.calcite.rel.metadata.RelMdRowCount.getRowCount(Aggregate rel,...)
        case COLLAPSE -> {
          dRows = dRows / 10;
          dCpu += dRows;
        }
          // Ignore cost the primitive filter but it will affect the rows count.
        case FILTER -> dRows =
            NumberUtil.multiply(
                dRows, RelMdUtil.guessSelectivity(((FilterDigest) operation.digest()).condition()));
        case SCRIPT -> {
          FilterDigest filterDigest = (FilterDigest) operation.digest();
          dRows = NumberUtil.multiply(dRows, RelMdUtil.guessSelectivity(filterDigest.condition()));
          // Calculate the cost of script filter by multiplying the selectivity of the filter and
          // the factor amplified by script count.
          dCpu += NumberUtil.multiply(dRows, Math.pow(1.1, filterDigest.scriptCount()));
        }
          // Ignore cost the LIMIT but it will affect the rows count.
          // Try to reduce the rows count by 1 to make the cost cheaper slightly than non-push down.
          // Because we'd like to push down LIMIT even when the fetch in LIMIT is greater than
          // dRows.
        case LIMIT -> dRows = Math.min(dRows, ((LimitDigest) operation.digest()).limit()) - 1;
      }
      ;
    }
    // Add the external cost to introduce the effect from FILTER, LIMIT and PROJECT.
    dCpu += dRows * getRowType().getFieldList().size();
    /*
     The impact factor to estimate the row count after push down an operator.

     <p>It will be multiplied to the original estimated row count of the operator, and it's set to
     less than 1 by default to make the result always less than the row count of operator without
     push down. As a result, the optimizer will prefer the plan with push down.
    */
    double estimateRowCountFactor =
        osIndex.getSettings().getSettingValue(CALCITE_PUSHDOWN_ROWCOUNT_ESTIMATION_FACTOR);
    return planner.getCostFactory().makeCost(dCpu * estimateRowCountFactor, 0, 0);
  }

  /** See source in {@link org.apache.calcite.rel.core.Aggregate::computeSelfCost} */
  private static float getAggMultiplier(PushDownOperation operation) {
    // START CALCITE
    List<AggregateCall> aggCalls = ((Aggregate) operation.digest()).getAggCallList();
    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.getAggregation().getName().equals("SUM")) {
        // Pretend that SUM costs a little bit more than $SUM0,
        // to make things deterministic.
        multiplier += 0.0125f;
      }
    }
    // END CALCITE

    // For script aggregation, we need to multiply the multiplier by 1.1 to make up the cost. As we
    // prefer to have non-script agg push down after optimized by {@link PPLAggregateConvertRule}
    multiplier *= (float) Math.pow(1.1f, ((AggPushDownAction) operation.action()).getScriptCount());
    return multiplier;
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
              pushDownContext.cloneWithoutSort());

      AbstractAction<?> action;
      Object digest;
      if (pushDownContext.isAggregatePushed()) {
        // Push down the sort into the aggregation bucket
        action =
            (AggregationBuilderAction)
                aggAction ->
                    aggAction.pushDownSortIntoAggBucket(collations, getRowType().getFieldNames());
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
        action = (OSRequestBuilderAction) requestBuilder -> requestBuilder.pushDownSort(builders);
        digest = builders.toString();
      }
      newScan.pushDownContext.add(PushDownType.SORT, digest, action);
      return newScan;
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cannot pushdown the sort {}", getCollationNames(collations), e);
      }
    }
    return null;
  }
}
