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
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableOSIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalOSIndexScan;

/**
 * Rule to convert a {@link CalciteLogicalOSIndexScan} to a {@link CalciteEnumerableOSIndexScan}.
 */
public class EnumerableIndexScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalOSIndexScan.class,
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
    CalciteLogicalOSIndexScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalOSIndexScan scan = (CalciteLogicalOSIndexScan) rel;
    return new CalciteEnumerableOSIndexScan(
        scan.getCluster(),
        scan.getHints(),
        scan.getTable(),
        scan.getOsIndex(),
        scan.getSchema(),
        scan.getPushDownContext());
  }
}
