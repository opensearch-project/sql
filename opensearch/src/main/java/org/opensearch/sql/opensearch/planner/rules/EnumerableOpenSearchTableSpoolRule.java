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
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.EnumerableOpenSearchTableSpool;

/**
 * Converts a {@link LogicalTableSpool} whose target is an {@link OpenSearchIndex} into the physical
 * {@link EnumerableOpenSearchTableSpool}.
 */
public class EnumerableOpenSearchTableSpoolRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              LogicalTableSpool.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableOpenSearchTableSpoolRule")
          .withRuleFactory(EnumerableOpenSearchTableSpoolRule::new);

  protected EnumerableOpenSearchTableSpoolRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LogicalTableSpool spool = call.rel(0);
    return spool.getTable() != null && spool.getTable().unwrap(OpenSearchIndex.class) != null;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final LogicalTableSpool spool = (LogicalTableSpool) rel;
    RelTraitSet traitSet = spool.getTraitSet().replace(EnumerableConvention.INSTANCE);
    RelNode convertedInput =
        convert(
            spool.getInput(),
            spool.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE));
    return new EnumerableOpenSearchTableSpool(
        spool.getCluster(), traitSet, convertedInput, spool.getTable());
  }
}
