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
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.write.EnumerableOutputLookup;
import org.opensearch.transport.client.node.NodeClient;

/** Lowers {@link OutputLookupTableModify} to the physical {@link EnumerableOutputLookup}. */
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
  public RelNode convert(RelNode rel) {
    OutputLookupTableModify node = (OutputLookupTableModify) rel;
    OpenSearchIndex index = indexOf(node);
    NodeClient client = index.getClient().getNodeClient().orElseThrow();
    int maxRows = index.getSettings().getSettingValue(Settings.Key.OUTPUTLOOKUP_MAX_ROWS);
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
        node.getMax(),
        maxRows);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    OutputLookupTableModify node = (OutputLookupTableModify) call.rel(0);
    OpenSearchIndex index = indexOf(node);
    return index != null && index.getClient().getNodeClient().isPresent();
  }

  private static OpenSearchIndex indexOf(OutputLookupTableModify node) {
    if (node.getTable() == null) {
      return null;
    }
    return node.getTable().unwrap(OpenSearchIndex.class);
  }
}
