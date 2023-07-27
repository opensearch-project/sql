/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import java.util.Collections;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.scan.QueryExemplarsFunctionTableScanBuilder;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.prometheus.storage.implementor.PrometheusDefaultImplementor;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * This is {@link Table} for querying exemplars in prometheus Table.
 * Since {@link PrometheusMetricTable} is overloaded with query_range and normal
 * PPL metric queries. Created a separate table for handling
 * {@link PrometheusQueryExemplarsRequest}
 */
@RequiredArgsConstructor
public class QueryExemplarsTable implements Table {

  @Getter
  private final PrometheusClient prometheusClient;

  @Getter
  private final PrometheusQueryExemplarsRequest exemplarsRequest;


  @Override
  public Map<String, ExprType> getFieldTypes() {
    return Collections.emptyMap();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new PrometheusDefaultImplementor(), null);
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    return new QueryExemplarsFunctionTableScanBuilder(prometheusClient, exemplarsRequest);
  }

}
