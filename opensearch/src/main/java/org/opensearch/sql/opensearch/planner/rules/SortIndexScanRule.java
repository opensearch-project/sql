/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import java.util.function.Predicate;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Sort;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

@Value.Enclosing
public class SortIndexScanRule extends RelRule<SortIndexScanRule.Config> {

  protected SortIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final AbstractCalciteIndexScan scan = call.rel(1);
    if (sort.getConvention() != scan.getConvention()) {
      return;
    }

    var collations = sort.collation.getFieldCollations();
    AbstractCalciteIndexScan newScan = scan.pushDownSort(collations);
    if (newScan != null) {
      call.transformTo(newScan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    SortIndexScanRule.Config DEFAULT =
        ImmutableSortIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(Sort.class)
                        .predicate(PlanUtils::sortByFieldsOnly)
                        .oneInput(
                            b1 ->
                                b1.operand(AbstractCalciteIndexScan.class)
                                    // Skip the rule if Top-K(i.e. sort + limit) has already been
                                    // pushed down. Otherwise,
                                    // Continue to push down sort although limit has already been
                                    // pushed down since we don't promise collation with only limit.
                                    .predicate(
                                        Predicate.not(AbstractCalciteIndexScan::isTopKPushed)
                                            .and(
                                                Predicate.not(
                                                    AbstractCalciteIndexScan
                                                        ::isMetricsOrderPushed)))
                                    .noInputs()));

    @Override
    default SortIndexScanRule toRule() {
      return new SortIndexScanRule(this);
    }
  }
}
