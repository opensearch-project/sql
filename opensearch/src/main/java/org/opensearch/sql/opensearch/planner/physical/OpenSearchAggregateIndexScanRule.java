/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
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
    if (call.rels.length == 3) {
      final LogicalAggregate aggregate = call.rel(0);
      final LogicalProject project = call.rel(1);
      final CalciteLogicalIndexScan scan = call.rel(2);

      // For multiple group-by, we currently have to use CompositeAggregationBuilder while it
      // doesn't support auto_date_histogram referring to bin command with parameter bins
      if (aggregate.getGroupSet().length() > 1 && Config.containsWidthBucketFuncOnDate(project)) {
        return;
      }

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
                                        Predicate.not(OpenSearchIndexScanRule::containsRexOver)
                                            .and(OpenSearchIndexScanRule::distinctProjectList)
                                            .or(Config::containsWidthBucketFuncOnDate))
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(
                                                    Predicate.not(
                                                            OpenSearchIndexScanRule::isLimitPushed)
                                                        .and(
                                                            OpenSearchIndexScanRule
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
                                        Predicate.not(OpenSearchIndexScanRule::isLimitPushed)
                                            .and(OpenSearchIndexScanRule::noAggregatePushed))
                                    .noInputs()));

    @Override
    default OpenSearchAggregateIndexScanRule toRule() {
      return new OpenSearchAggregateIndexScanRule(this);
    }

    static boolean containsWidthBucketFuncOnDate(LogicalProject project) {
      return project.getProjects().stream()
          .anyMatch(
              expr ->
                  expr instanceof RexCall rexCall
                      && rexCall.getOperator().equals(WIDTH_BUCKET)
                      && dateRelatedType(rexCall.getOperands().getFirst().getType()));
    }

    static boolean dateRelatedType(RelDataType type) {
      return type instanceof ExprSqlType exprSqlType
          && List.of(ExprUDT.EXPR_DATE, ExprUDT.EXPR_TIME, ExprUDT.EXPR_TIMESTAMP)
              .contains(exprSqlType.getUdt());
    }
  }
}
