/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.immutables.value.Value;
import org.opensearch.sql.calcite.plan.OpenSearchRuleConfig;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableTopK;

/**
 * Rule to convert an {@link LogicalSort} with ({@link LogicalSort#fetch} or {@link
 * LogicalSort#offset}) and {@link LogicalSort#collation}(Order By) into an {@link
 * CalciteEnumerableTopK}.
 */
@Value.Enclosing
public class EnumerableTopKConverterRule
    extends InterruptibleRelRule<EnumerableTopKConverterRule.Config> {
  protected EnumerableTopKConverterRule(EnumerableTopKConverterRule.Config config) {
    super(config);
  }

  @Override
  protected void onMatchImpl(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    RelNode input = sort.getInput();
    final Sort topK =
        CalciteEnumerableTopK.create(
            convert(
                call.getPlanner(),
                input,
                input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
            sort.getCollation(),
            sort.offset,
            sort.fetch);
    call.transformTo(topK);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends OpenSearchRuleConfig {
    EnumerableTopKConverterRule.Config DEFAULT =
        ImmutableEnumerableTopKConverterRule.Config.builder()
            .build()
            .withDescription("EnumerableTopK")
            .withOperandSupplier(
                b0 ->
                    b0.operand(LogicalSort.class)
                        .predicate(
                            sort ->
                                (sort.fetch != null || sort.offset != null)
                                    && !sort.collation.getFieldCollations().isEmpty())
                        .anyInputs());

    @Override
    default EnumerableTopKConverterRule toRule() {
      return new EnumerableTopKConverterRule(this);
    }
  }
}
