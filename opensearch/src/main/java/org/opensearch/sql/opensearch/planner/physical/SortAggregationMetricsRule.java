/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;
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
    final CalciteLogicalIndexScan scan = call.rel(1);
    // Only support single metric sort
    if (sort.getCollation().getFieldCollations().size() != 1) {
      return;
    }
    CalciteLogicalIndexScan newScan = scan.pushDownSortAggregateMetrics(sort);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    SortAggregationMetricsRule.Config DEFAULT =
        ImmutableSortAggregationMetricsRule.Config.builder()
            .build()
            .withDescription("Sort-TableScan(agg-pushed)")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(PlanUtils::sortByFieldsOnly)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));

    @Override
    default SortAggregationMetricsRule toRule() {
      return new SortAggregationMetricsRule(this);
    }
  }
}
