/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Planner rule that push a {@link LogicalFilter} down to {@link CalciteLogicalIndexScan} */
@Value.Enclosing
public class OpenSearchFilterIndexScanRule extends RelRule<OpenSearchFilterIndexScanRule.Config> {

  /** Creates a OpenSearchFilterIndexScanRule. */
  protected OpenSearchFilterIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
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
    CalciteLogicalIndexScan newScan = scan.pushDownFilter(filter);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Filter on CalciteLogicalIndexScan. */
    Config DEFAULT =
        ImmutableOpenSearchFilterIndexScanRule.Config.builder()
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
                                        Predicate.not(OpenSearchIndexScanRule::isLimitPushed)
                                            .and(OpenSearchIndexScanRule::test))
                                    .noInputs()));

    @Override
    default OpenSearchFilterIndexScanRule toRule() {
      return new OpenSearchFilterIndexScanRule(this);
    }
  }
}
