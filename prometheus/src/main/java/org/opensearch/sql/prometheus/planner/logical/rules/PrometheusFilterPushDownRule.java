/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.prometheus.storage.scan.CalciteLogicalPrometheusScan;

/**
 * Planner rule that pushes filter conditions (time range and label matchers) down into a {@link
 * CalciteLogicalPrometheusScan}.
 *
 * <p>Supported pushdowns:
 *
 * <ul>
 *   <li>Time range comparisons on @timestamp (>, >=, <, <=)
 *   <li>Label equality conditions (label = 'value')
 * </ul>
 *
 * <p>Unsupported conditions remain as a LogicalFilter on top.
 */
public class PrometheusFilterPushDownRule extends RelOptRule {

  public static final PrometheusFilterPushDownRule INSTANCE =
      new PrometheusFilterPushDownRule(
          operand(
              LogicalFilter.class,
              operand(CalciteLogicalPrometheusScan.class, none())),
          "PrometheusFilterPushDownRule");

  private PrometheusFilterPushDownRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final CalciteLogicalPrometheusScan scan = call.rel(1);

    RelNode newNode = scan.pushDownFilter(filter.getCondition());
    if (newNode != null) {
      call.transformTo(newNode);
      PlanUtils.tryPruneRelNodes(call);
    }
  }
}
