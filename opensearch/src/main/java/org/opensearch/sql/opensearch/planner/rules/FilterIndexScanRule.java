/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that push a {@link LogicalFilter} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class FilterIndexScanRule extends InterruptibleRelRule<FilterIndexScanRule.Config> {

  /** Creates a FilterIndexScanRule. */
  protected FilterIndexScanRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final LogicalFilter filter = call.rel(0);
      final CalciteLogicalIndexScan scan = call.rel(1);
      apply(call, filter, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, Filter filter, CalciteLogicalIndexScan scan) {
    AbstractRelNode newRel = scan.pushDownFilter(filter);
    if (newRel != null) {
      call.transformTo(newRel);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    /** Config that matches Filter on CalciteLogicalIndexScan. */
    Config DEFAULT =
        ImmutableFilterIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(
                                        // Filter pushdown is skipped if a limit has already been
                                        // pushed down because the current DSL cannot correctly
                                        // handle filter pushdown after limit. Both "limit after
                                        // filter" and "filter after limit" result in the same
                                        // limit-after-filter DSL.
                                        Predicate.not(AbstractCalciteIndexScan::isLimitPushed)
                                            .and(AbstractCalciteIndexScan::noAggregatePushed))
                                    .noInputs()));

    @Override
    default FilterIndexScanRule toRule() {
      return new FilterIndexScanRule(this);
    }
  }
}
