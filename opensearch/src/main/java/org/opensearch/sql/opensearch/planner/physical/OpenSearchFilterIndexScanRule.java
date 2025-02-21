/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteOpenSearchIndexScan;

/** Planner rule that push a {@link Filter} down to {@link CalciteOpenSearchIndexScan} */
@Value.Enclosing
public class OpenSearchFilterIndexScanRule extends RelRule<OpenSearchFilterIndexScanRule.Config> {

  /** Creates a OpenSearchFilterIndexScanRule. */
  protected OpenSearchFilterIndexScanRule(Config config) {
    super(config);
  }

  protected static boolean test(CalciteOpenSearchIndexScan scan) {
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final Filter filter = call.rel(0);
      final CalciteOpenSearchIndexScan scan = call.rel(1);
      apply(call, filter, scan);
    } else {
      throw new AssertionError(
          String.format(
              "The length of rels should be %s but got %s",
              this.operands.size(), call.rels.length));
    }
  }

  protected void apply(RelOptRuleCall call, Filter filter, CalciteOpenSearchIndexScan scan) {
    if (scan.pushDownFilter(filter)) {
      call.transformTo(scan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Filter on CalciteOpenSearchIndexScan. */
    Config DEFAULT =
        ImmutableOpenSearchFilterIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(Filter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteOpenSearchIndexScan.class)
                                    .predicate(OpenSearchFilterIndexScanRule::test)
                                    .noInputs()));

    @Override
    default OpenSearchFilterIndexScanRule toRule() {
      return new OpenSearchFilterIndexScanRule(this);
    }
  }
}
