/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@Value.Enclosing
public class OpenSearchSortIndexScanRule extends RelRule<OpenSearchSortIndexScanRule.Config> {

  protected OpenSearchSortIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalSort sort = call.rel(0);
    final CalciteLogicalIndexScan scan = call.rel(1);

    var collations = sort.collation.getFieldCollations();
    CalciteLogicalIndexScan newScan = scan.pushDownSort(collations);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    OpenSearchSortIndexScanRule.Config DEFAULT =
        ImmutableOpenSearchSortIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(OpenSearchIndexScanRule::sortByFieldsOnly)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteLogicalIndexScan.class)
                                    .predicate(OpenSearchIndexScanRule::noAggregatePushed)
                                    .noInputs()));

    @Override
    default OpenSearchSortIndexScanRule toRule() {
      return new OpenSearchSortIndexScanRule(this);
    }
  }
}
