/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.OpenSearchRuleConfig;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class HavingPushdownRule extends RelRule<HavingPushdownRule.Config> {
  protected HavingPushdownRule(HavingPushdownRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final LogicalProject project = call.rel(1);
    final CalciteLogicalIndexScan scan = call.rel(2);
    CalciteLogicalIndexScan newScan = scan.pushDownHavingClauseFlag(filter);
    if (newScan != null) {
      call.transformTo(
          call.builder()
              .push(newScan)
              .project(project.getProjects())
              .filter(filter.getCondition())
              .build());
    }
  }

  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    HavingPushdownRule.Config DEFAULT =
        ImmutableHavingPushdownRule.Config.builder()
            .build()
            .withDescription("Filter-Project-TableScan(AggPushed)")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(LogicalProject.class)
                                    .oneInput(
                                        b2 ->
                                            b2.operand(CalciteLogicalIndexScan.class)
                                                .predicate(
                                                    Predicate.not(
                                                            AbstractCalciteIndexScan
                                                                ::noAggregatePushed)
                                                        .and(
                                                            Predicate.not(
                                                                AbstractCalciteIndexScan
                                                                    ::isHavingFlagPushed)))
                                                .noInputs())));

    @Override
    default HavingPushdownRule toRule() {
      return new HavingPushdownRule(this);
    }
  }
}
