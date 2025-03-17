/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

/** Rule to convert a {@link CalciteLogicalIndexScan} to a {@link CalciteEnumerableIndexScan}. */
public class EnumerableIndexScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalIndexScan.class,
              s -> s.getOsIndex() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableIndexScanRule")
          .withRuleFactory(EnumerableIndexScanRule::new);

  /** Creates an EnumerableProjectRule. */
  protected EnumerableIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    CalciteLogicalIndexScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalIndexScan scan = (CalciteLogicalIndexScan) rel;
    return new CalciteEnumerableIndexScan(
        scan.getCluster(),
        scan.getHints(),
        scan.getTable(),
        scan.getOsIndex(),
        scan.getSchema(),
        scan.getPushDownContext());
  }
}
