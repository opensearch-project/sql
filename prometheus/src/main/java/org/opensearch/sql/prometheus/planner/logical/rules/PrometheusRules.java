/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;

/** Registry of Calcite planner rules for Prometheus scan optimization. */
public class PrometheusRules {

  private PrometheusRules() {
    // Utility class
  }

  /** All Prometheus-specific planner rules. */
  public static final List<RelOptRule> PROMETHEUS_RULES =
      ImmutableList.of(
          // Converter rule: logical scan -> physical scan
          EnumerablePrometheusScanRule.DEFAULT_CONFIG.toRule(),
          // Filter pushdown: time range + label matchers
          PrometheusFilterPushDownRule.INSTANCE);
}
