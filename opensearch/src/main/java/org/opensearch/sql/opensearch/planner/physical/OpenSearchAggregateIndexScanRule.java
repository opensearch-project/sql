/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.immutables.value.Value;
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
      apply(call, aggregate, project, scan);
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
    CalciteLogicalIndexScan newScan = scan.pushDownAggregate(aggregate, project);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT =
        ImmutableOpenSearchAggregateIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .predicate(OpenSearchIndexScanRule::distinctProjectList)
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

    @Override
    default OpenSearchAggregateIndexScanRule toRule() {
      return new OpenSearchAggregateIndexScanRule(this);
    }
  }
}
