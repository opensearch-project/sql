/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.scan;

import lombok.AllArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * TableScanBuilder for query_range table function of prometheus connector. we can merge this when
 * we refactor for existing ppl queries based on prometheus connector.
 */
@AllArgsConstructor
public class QueryRangeFunctionTableScanBuilder extends TableScanBuilder {

  private final PrometheusClient prometheusClient;

  private final PrometheusQueryRequest prometheusQueryRequest;

  @Override
  public TableScanOperator build() {
    return new QueryRangeFunctionTableScanOperator(prometheusClient, prometheusQueryRequest);
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    return true;
  }
}
