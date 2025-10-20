/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.immutables.value.Value;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class SortAggregationMetricsRule extends RelRule<SortAggregationMetricsRule.Config> {

  protected SortAggregationMetricsRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final LogicalProject projectAddedBySort = call.rel(1);
    final LogicalAggregate aggregate = call.rel(2);
    final LogicalFilter filter = call.rel(3);
    final LogicalProject project = call.rel(4);
    final CalciteLogicalIndexScan scan = call.rel(5);
    // Only support single metric sort
    if (sort.getCollation().getFieldCollations().size() != 1) {
      return;
    }
    // Only support single metric in aggregate
    if (aggregate.getAggCallList().size() != 1) {
      return;
    }
    int possibleMetricsIndexInSort =
        sort.getCollation().getFieldCollations().getFirst().getFieldIndex();
    RexNode possibleMetricsInProject =
        projectAddedBySort.getProjects().get(possibleMetricsIndexInSort);
    if (!(possibleMetricsInProject instanceof RexInputRef inputRef)) {
      return;
    }
    int possibleMetricsIndexInProject = inputRef.getIndex();
    RelDataTypeField possibleMetricsInAggregate =
        aggregate.getRowType().getFieldList().get(possibleMetricsIndexInProject);
    if (possibleMetricsInAggregate.getType().getSqlTypeName().getFamily()
        != SqlTypeFamily.NUMERIC) {
      return;
    }
    if (!aggregate
        .getAggCallList()
        .getFirst()
        .getName()
        .equals(possibleMetricsInAggregate.getName())) {
      return;
    }
    List<Integer> groupSet = aggregate.getGroupSet().asList();
    RexNode condition = filter.getCondition();
    Function<RexNode, Boolean> isNotNullFromAgg =
        rex ->
            rex instanceof RexCall rexCall
                && rexCall.getOperator() == SqlStdOperatorTable.IS_NOT_NULL
                && rexCall.getOperands().get(0) instanceof RexInputRef ref
                && groupSet.contains(ref.getIndex());
    if (isNotNullFromAgg.apply(condition)
        || (condition instanceof RexCall rexCall
            && rexCall.getOperator() == SqlStdOperatorTable.AND
            && rexCall.getOperands().stream().allMatch(isNotNullFromAgg::apply))) {
      // Try to do the aggregate push down and ignore the filter if the filter sources from the
      // aggregate's hint. See{@link CalciteRelNodeVisitor::visitAggregation}
      RelFieldCollation.Direction direction =
          sort.getCollation().getFieldCollations().getFirst().direction;
      apply(call, projectAddedBySort, aggregate, project, scan, direction);
    }
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalProject projectAddedBySort,
      LogicalAggregate aggregate,
      LogicalProject project,
      CalciteLogicalIndexScan scan,
      RelFieldCollation.Direction metricOrder) {
    AbstractRelNode newScan = scan.pushDownAggregate(aggregate, project, metricOrder);
    if (newScan != null) {
      RelNode newScanWithProject =
          call.builder().push(newScan).project(projectAddedBySort.getProjects()).build();
      call.transformTo(newScanWithProject);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    SortAggregationMetricsRule.Config DEFAULT =
        ImmutableSortAggregationMetricsRule.Config.builder()
            .build()
            .withDescription("Sort-Project-Agg-Filter-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(PlanUtils::sortByFieldsOnly)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalAggregate.class)
                                                .predicate(
                                                    agg ->
                                                        agg.getHints().stream()
                                                            .anyMatch(
                                                                hint ->
                                                                    hint.hintName.equals(
                                                                            "stats_args")
                                                                        && hint.kvOptions
                                                                            .get(
                                                                                Argument
                                                                                    .BUCKET_NULLABLE)
                                                                            .equals("false")))
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(LogicalFilter.class)
                                                            .predicate(
                                                                OpenSearchAggregateIndexScanRule
                                                                        .Config
                                                                    ::mayBeFilterFromBucketNonNull)
                                                            .oneInput(
                                                                b4 ->
                                                                    b4.operand(LogicalProject.class)
                                                                        .predicate(
                                                                            Predicate.not(
                                                                                    PlanUtils
                                                                                        ::containsRexOver)
                                                                                .and(
                                                                                    PlanUtils
                                                                                        ::distinctProjectList)
                                                                                .or(
                                                                                    OpenSearchAggregateIndexScanRule
                                                                                            .Config
                                                                                        ::containsWidthBucketFuncOnDate))
                                                                        .oneInput(
                                                                            b5 ->
                                                                                b5.operand(
                                                                                        CalciteLogicalIndexScan
                                                                                            .class)
                                                                                    .predicate(
                                                                                        Predicate
                                                                                            .not(
                                                                                                AbstractCalciteIndexScan
                                                                                                    ::isLimitPushed)
                                                                                            .and(
                                                                                                AbstractCalciteIndexScan
                                                                                                    ::noAggregatePushed))
                                                                                    .noInputs()))))));

    @Override
    default SortAggregationMetricsRule toRule() {
      return new SortAggregationMetricsRule(this);
    }
  }
}
