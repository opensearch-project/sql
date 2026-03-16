/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rel.LogicalHighlight;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/**
 * Planner rule that pushes a {@link LogicalHighlight} down to {@link CalciteLogicalIndexScan}. This
 * adds the {@code _highlight} column to the scan schema and configures the OpenSearch {@code
 * HighlightBuilder} for the search request.
 */
@Value.Enclosing
public class HighlightIndexScanRule extends InterruptibleRelRule<HighlightIndexScanRule.Config> {

  protected HighlightIndexScanRule(Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final LogicalHighlight highlight = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);
    RelNode newScan = scan.pushDownHighlight(highlight.getHighlightConfig());
    if (newScan != null) {
      call.transformTo(newScan);
      PlanUtils.tryPruneRelNodes(call);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    Config DEFAULT =
        ImmutableHighlightIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalHighlight.class)
                        .oneInput(b1 -> b1.operand(CalciteLogicalIndexScan.class).noInputs()));

    @Override
    default HighlightIndexScanRule toRule() {
      return new HighlightIndexScanRule(this);
    }
  }
}
