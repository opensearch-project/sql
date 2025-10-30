/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.immutables.value.Value;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.expression.function.udf.binning.WidthBucketFunction;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that push a {@link LogicalAggregate} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class OpenSearchAggregateIndexScanRule
    extends RelRule<OpenSearchAggregateIndexScanRule.Config> {

  /** Creates a OpenSearchAggregateIndexScanRule. */
  protected OpenSearchAggregateIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 4) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalFilter filter = call.rel(1);
      final LogicalProject project = call.rel(2);
      final CalciteLogicalIndexScan scan = call.rel(3);
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
        apply(call, aggregate, project, scan);
      }
    } else if (call.rels.length == 3) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalProject project = call.rel(1);
      final CalciteLogicalIndexScan scan = call.rel(2);
      apply(call, aggregate, project, scan);
    } else if (call.rels.length == 2) {
      // case of count() without group-by
      final LogicalAggregate aggregate = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);
      apply(call, aggregate, null, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(
      RelOptRuleCall call,
      LogicalAggregate aggregate,
      LogicalProject project,
      CalciteLogicalIndexScan scan) {
    AbstractRelNode newRelNode = scan.pushDownAggregate(aggregate, project);
    if (newRelNode != null) {
      call.transformTo(newRelNode);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableOpenSearchAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(
                                        // Support push down aggregate with project that:
                                        // 1. No RexOver and no duplicate projection
                                        // 2. Contains width_bucket function on date field referring
                                        // to bin command with parameter bins
                                        Predicate.not(PlanUtils::containsRexOver)
                                            .and(PlanUtils::distinctProjectList)
                                            .or(Config::containsWidthBucketFuncOnDate))
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(
                                                    Predicate.not(
                                                            AbstractCalciteIndexScan::isLimitPushed)
                                                        .and(
                                                            AbstractCalciteIndexScan
                                                                ::noAggregatePushed))
                                                .noInputs())));
    Config COUNT_STAR =
        ImmutableOpenSearchAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg[count()]-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(
                            agg ->
                                agg.getGroupSet().isEmpty()
                                    && agg.getAggCallList().stream()
                                        .allMatch(
                                            call ->
                                                call.getAggregation().kind == SqlKind.COUNT
                                                    && call.getArgList().isEmpty()))
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::isLimitPushed)
                                            .and(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));
    // TODO: No need this rule once https://github.com/opensearch-project/sql/issues/4403 is
    // addressed
    Config BUCKET_NON_NULL_AGG =
        ImmutableOpenSearchAggregateIndexScanRule.Config.builder()
            .build()
            .withDescription("Agg-Filter-Project-TableScan")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .predicate(
                            agg ->
                                agg.getHints().stream()
                                    .anyMatch(
                                        hint ->
                                            hint.hintName.equals("stats_args")
                                                && hint.kvOptions
                                                    .get(Argument.BUCKET_NULLABLE)
                                                    .equals("false")))
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalFilter.class)
                                    .predicate(Config::mayBeFilterFromBucketNonNull)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(LogicalProject.class)
                                                .predicate(
                                                    // Support push down aggregate with project
                                                    // that:
                                                    // 1. No RexOver and no duplicate projection
                                                    // 2. Contains width_bucket function on date
                                                    // field referring
                                                    // to bin command with parameter bins
                                                    Predicate.not(PlanUtils::containsRexOver)
                                                        .and(PlanUtils::distinctProjectList)
                                                        .or(Config::containsWidthBucketFuncOnDate))
                                                .oneInput(
                                                    b3 ->
                                                        b3.operand(CalciteLogicalIndexScan.class)
                                                            .predicate(
                                                                Predicate.not(
                                                                        AbstractCalciteIndexScan
                                                                            ::isLimitPushed)
                                                                    .and(
                                                                        AbstractCalciteIndexScan
                                                                            ::noAggregatePushed))
                                                            .noInputs()))));

    @Override
    default OpenSearchAggregateIndexScanRule toRule() {
      return new OpenSearchAggregateIndexScanRule(this);
    }

    static boolean mayBeFilterFromBucketNonNull(LogicalFilter filter) {
      RexNode condition = filter.getCondition();
      return isNotNullOnRef(condition)
          || (condition instanceof RexCall rexCall
              && rexCall.getOperator().equals(SqlStdOperatorTable.AND)
              && rexCall.getOperands().stream()
                  .allMatch(OpenSearchAggregateIndexScanRule.Config::isNotNullOnRef));
    }

    private static boolean isNotNullOnRef(RexNode rex) {
      return rex instanceof RexCall rexCall
          && rexCall.getOperator().equals(SqlStdOperatorTable.IS_NOT_NULL)
          && rexCall.getOperands().get(0) instanceof RexInputRef;
    }

    static boolean containsWidthBucketFuncOnDate(LogicalProject project) {
      return project.getProjects().stream()
          .anyMatch(
              expr ->
                  expr instanceof RexCall rexCall
                      && rexCall.getOperator().equals(WIDTH_BUCKET)
                      && WidthBucketFunction.dateRelatedType(
                          rexCall.getOperands().getFirst().getType()));
    }
  }
}
