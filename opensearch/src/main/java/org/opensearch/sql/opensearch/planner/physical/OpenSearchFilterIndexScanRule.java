/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalOSIndexScan;

/** Planner rule that push a {@link LogicalFilter} down to {@link CalciteLogicalOSIndexScan} */
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
      final CalciteLogicalOSIndexScan scan = call.rel(1);
      apply(call, filter, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, Filter filter, CalciteLogicalOSIndexScan scan) {
    CalciteLogicalOSIndexScan newScan = scan.pushDownFilter(filter);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Filter on CalciteLogicalOSIndexScan. */
    Config DEFAULT =
        ImmutableOpenSearchFilterIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalFilter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalOSIndexScan.class)
                                    .predicate(OpenSearchIndexScanRule::test)
                                    .noInputs()));

    @Override
    default OpenSearchFilterIndexScanRule toRule() {
      return new OpenSearchFilterIndexScanRule(this);
    }
  }
}
