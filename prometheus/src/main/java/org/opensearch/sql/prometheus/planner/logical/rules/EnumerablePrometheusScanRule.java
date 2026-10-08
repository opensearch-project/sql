/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.opensearch.sql.prometheus.storage.scan.CalciteEnumerablePrometheusScan;
import org.opensearch.sql.prometheus.storage.scan.CalciteLogicalPrometheusScan;

/**
 * Rule to convert a {@link CalciteLogicalPrometheusScan} to a {@link
 * CalciteEnumerablePrometheusScan}.
 */
public class EnumerablePrometheusScanRule extends ConverterRule {

  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              CalciteLogicalPrometheusScan.class,
              s -> s.getPrometheusTable() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerablePrometheusScanRule")
          .withRuleFactory(EnumerablePrometheusScanRule::new);

  /** Creates an EnumerablePrometheusScanRule. */
  protected EnumerablePrometheusScanRule(Config config) {
    super(config);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final CalciteLogicalPrometheusScan scan = (CalciteLogicalPrometheusScan) rel;
    return new CalciteEnumerablePrometheusScan(
        scan.getCluster(),
        scan.getTraitSet().plus(EnumerableConvention.INSTANCE),
        scan.getTable(),
        scan.getPrometheusTable(),
        scan.getSchema(),
        scan.getPushDownContext());
  }
}
