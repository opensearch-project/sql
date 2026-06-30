/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.opensearch.storage.rest.CalciteEnumerableRestScan;
import org.opensearch.sql.opensearch.storage.rest.CalciteLogicalRestScan;

/** Rule to convert a {@link CalciteLogicalRestScan} to a {@link CalciteEnumerableRestScan}. */
public class EnumerableRestScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalRestScan.class,
              s -> s.getRestTable() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableRestScanRule")
          .withRuleFactory(EnumerableRestScanRule::new);

  protected EnumerableRestScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    CalciteLogicalRestScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalRestScan scan = (CalciteLogicalRestScan) rel;
    return new CalciteEnumerableRestScan(
        scan.getCluster(), scan.getHints(), scan.getTable(), scan.getRestTable(), scan.getSchema());
  }
}
