/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.functions.scan;

import lombok.AllArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * TableScanBuilder for query_exemplars table function of prometheus connector.
 */
@AllArgsConstructor
public class QueryExemplarsFunctionTableScanBuilder extends TableScanBuilder {

  private final PrometheusClient prometheusClient;

  private final PrometheusQueryExemplarsRequest prometheusQueryExemplarsRequest;

  @Override
  public TableScanOperator build() {
    return new QueryExemplarsFunctionTableScanOperator(prometheusClient,
        prometheusQueryExemplarsRequest);
  }

  // Since we are determining the schema after table scan,
  // we are ignoring default Logical Project added in the plan.
  @Override
  public boolean pushDownProject(LogicalProject project) {
    return true;
  }

}
