/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableProject;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalTableScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteOpenSearchIndexScan;

/**
 * Rule to convert a {@link LogicalProject} to an {@link EnumerableProject}. You may provide a
 * custom config to convert other nodes that extend {@link Project}.
 *
 * @see EnumerableRules#ENUMERABLE_PROJECT_RULE
 */
public class EnumerableIndexScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalTableScan.class,
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
    CalciteLogicalTableScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalTableScan scan = (CalciteLogicalTableScan) rel;
    return new CalciteOpenSearchIndexScan(scan.getCluster(), scan.getTable(), scan.getOsIndex());
  }
}
