/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

@Value.Enclosing
public class OpenSearchSortIndexScanRule extends RelRule<OpenSearchSortIndexScanRule.Config> {

  protected OpenSearchSortIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final AbstractCalciteIndexScan scan = call.rel(1);

    var collations = sort.collation.getFieldCollations();
    AbstractCalciteIndexScan newScan = scan.pushDownSort(collations);
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
                    b0.operand(Sort.class)
                        .predicate(OpenSearchIndexScanRule::sortByFieldsOnly)
                        .oneInput(
                            b1 ->
                                b1.operand(AbstractCalciteIndexScan.class)
                                    // Skip the rule if a limit has already been pushed down
                                    // because pushing down a sort after a limit will be treated
                                    // as sort-then-limit by OpenSearch DSL.
                                    .predicate(
                                        Predicate.not(OpenSearchIndexScanRule::isLimitPushed))
                                    .noInputs()));

    @Override
    default OpenSearchSortIndexScanRule toRule() {
      return new OpenSearchSortIndexScanRule(this);
    }
  }
}
