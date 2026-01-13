/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptRuleCall;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRuleConfig;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableTopK;

/**
 * Rule to merge an {@link EnumerableLimit} and an ({@link EnumerableSort} into an {@link
 * CalciteEnumerableTopK}.
 */
@Value.Enclosing
public class EnumerableTopKMergeRule extends InterruptibleRelRule<EnumerableTopKMergeRule.Config> {
  protected EnumerableTopKMergeRule(EnumerableTopKMergeRule.Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final EnumerableLimit limit = call.rel(0);
    final EnumerableSort sort = call.rel(1);
    final CalciteEnumerableTopK topK =
        CalciteEnumerableTopK.create(
            sort.getInput(), sort.getCollation(), limit.offset, limit.fetch);
    call.transformTo(topK);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    EnumerableTopKMergeRule.Config DEFAULT =
        ImmutableEnumerableTopKMergeRule.Config.builder()
            .build()
            .withDescription("EnumerableTopKMerge")
            .withOperandSupplier(
                b0 ->
                    b0.operand(EnumerableLimit.class)
                        .oneInput(
                            b1 ->
                                b1.operand(EnumerableSort.class)
                                    .predicate(sort -> sort.fetch == null)
                                    .anyInputs()));

    @Override
    default EnumerableTopKMergeRule toRule() {
      return new EnumerableTopKMergeRule(this);
    }
  }
}
