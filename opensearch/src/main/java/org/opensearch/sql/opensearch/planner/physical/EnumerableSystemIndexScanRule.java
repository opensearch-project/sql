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
import org.opensearch.sql.opensearch.storage.system.CalciteEnumerableSystemIndexScan;
import org.opensearch.sql.opensearch.storage.system.CalciteLogicalSystemIndexScan;

/**
 * Rule to convert a {@link CalciteLogicalSystemIndexScan} to a {@link
 * CalciteEnumerableSystemIndexScan}.
 */
public class EnumerableSystemIndexScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalSystemIndexScan.class,
              s -> s.getSysIndex() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableSystemIndexScanRule")
          .withRuleFactory(EnumerableSystemIndexScanRule::new);

  /** Creates an EnumerableProjectRule. */
  protected EnumerableSystemIndexScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    CalciteLogicalSystemIndexScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalSystemIndexScan scan = (CalciteLogicalSystemIndexScan) rel;
    return new CalciteEnumerableSystemIndexScan(
        scan.getCluster(), scan.getHints(), scan.getTable(), scan.getSysIndex(), scan.getSchema());
  }
}
