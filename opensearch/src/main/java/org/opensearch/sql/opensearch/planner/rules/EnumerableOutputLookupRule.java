/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.calcite.OutputLookupTableModify;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.EnumerableOutputLookup;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Lowers an {@link OutputLookupTableModify} whose referenced table is an {@link OpenSearchIndex}
 * into the physical {@link EnumerableOutputLookup}, wiring in the in-cluster node client.
 */
public class EnumerableOutputLookupRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              OutputLookupTableModify.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableOutputLookupRule")
          .withRuleFactory(EnumerableOutputLookupRule::new);

  protected EnumerableOutputLookupRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return nodeClientOf((OutputLookupTableModify) call.rel(0)) != null;
  }

  @Override
  public RelNode convert(RelNode rel) {
    OutputLookupTableModify node = (OutputLookupTableModify) rel;
    NodeClient client = nodeClientOf(node);
    RelTraitSet traitSet = node.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode convertedInput =
        convert(
            node.getInput(), node.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE));
    return new EnumerableOutputLookup(
        node.getCluster(),
        traitSet,
        convertedInput,
        node.getRowType(),
        client,
        node.getIndexName(),
        node.isAppend(),
        node.isOverrideIfEmpty(),
        node.getKeyFields(),
        node.getMax());
  }

  private static NodeClient nodeClientOf(OutputLookupTableModify node) {
    if (node.getTable() == null) {
      return null;
    }
    OpenSearchIndex index = node.getTable().unwrap(OpenSearchIndex.class);
    if (index == null) {
      return null;
    }
    return index.getClient().getNodeClient().orElse(null);
  }
}
