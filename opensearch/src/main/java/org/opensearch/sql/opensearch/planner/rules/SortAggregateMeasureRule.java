/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class SortAggregateMeasureRule
    extends InterruptibleRelRule<SortAggregateMeasureRule.Config> {

  protected SortAggregateMeasureRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);
    CalciteLogicalIndexScan newScan = scan.pushDownSortAggregateMeasure(sort);
    if (newScan != null) {
      call.transformTo(newScan);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    // TODO support multiple measures, only support single measure sort
    Predicate<Sort> hasOneFieldCollation =
        sort -> sort.getCollation().getFieldCollations().size() == 1;
    SortAggregateMeasureRule.Config DEFAULT =
        ImmutableSortAggregateMeasureRule.Config.builder()
            .build()
            .withDescription("Sort-TableScan(agg-pushed)")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(hasOneFieldCollation.and(PlanUtils::sortByFieldsOnly))
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));

    @Override
    default SortAggregateMeasureRule toRule() {
      return new SortAggregateMeasureRule(this);
    }
  }
}
