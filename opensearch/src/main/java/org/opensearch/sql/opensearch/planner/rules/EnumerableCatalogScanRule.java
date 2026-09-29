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
import org.opensearch.sql.opensearch.storage.system.CalciteEnumerableCatalogScan;
import org.opensearch.sql.opensearch.storage.system.CalciteLogicalCatalogScan;
import org.opensearch.sql.opensearch.storage.system.CalciteScannableCatalogScan;

/**
 * Rule to convert a {@link CalciteLogicalCatalogScan} into an enumerable scan: a {@link
 * CalciteScannableCatalogScan} when the source opts into {@code Scannable}, otherwise a plain
 * {@link CalciteEnumerableCatalogScan}.
 */
public class EnumerableCatalogScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalCatalogScan.class,
              s -> s.getCatalogTable() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableCatalogScanRule")
          .withRuleFactory(EnumerableCatalogScanRule::new);

  protected EnumerableCatalogScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    CalciteLogicalCatalogScan scan = call.rel(0);
    return scan.getVariablesSet().isEmpty();
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalCatalogScan scan = (CalciteLogicalCatalogScan) rel;
    if (scan.getCatalogTable().getSource().isScannable()) {
      return new CalciteScannableCatalogScan(
          scan.getCluster(),
          scan.getHints(),
          scan.getTable(),
          scan.getCatalogTable(),
          scan.getSchema());
    }
    return new CalciteEnumerableCatalogScan(
        scan.getCluster(),
        scan.getHints(),
        scan.getTable(),
        scan.getCatalogTable(),
        scan.getSchema());
  }
}
