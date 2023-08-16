/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;

import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.scan.QueryExemplarsFunctionTableScanBuilder;
import org.opensearch.sql.prometheus.functions.scan.QueryExemplarsFunctionTableScanOperator;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
class QueryExemplarsTableTest {

  @Mock
  private PrometheusClient client;

  @Test
  @SneakyThrows
  void testGetFieldTypes() {
    PrometheusQueryExemplarsRequest exemplarsRequest
        = new PrometheusQueryExemplarsRequest();
    exemplarsRequest.setQuery(QUERY);
    exemplarsRequest.setStartTime(STARTTIME);
    exemplarsRequest.setEndTime(ENDTIME);
    QueryExemplarsTable queryExemplarsTable = new QueryExemplarsTable(client, exemplarsRequest);
    Map<String, ExprType> fieldTypes = queryExemplarsTable.getFieldTypes();
    Assertions.assertNotNull(fieldTypes);
    assertEquals(0, fieldTypes.size());
  }

  @Test
  void testImplementWithBasicMetricQuery() {

    PrometheusQueryExemplarsRequest exemplarsRequest
        = new PrometheusQueryExemplarsRequest();
    exemplarsRequest.setQuery(QUERY);
    exemplarsRequest.setStartTime(STARTTIME);
    exemplarsRequest.setEndTime(ENDTIME);
    LogicalPlan logicalPlan = new QueryExemplarsFunctionTableScanBuilder(client, exemplarsRequest);
    QueryExemplarsTable queryExemplarsTable = new QueryExemplarsTable(client, exemplarsRequest);
    PhysicalPlan physicalPlan = queryExemplarsTable.implement(logicalPlan);

    assertTrue(physicalPlan instanceof QueryExemplarsFunctionTableScanOperator);
    QueryExemplarsFunctionTableScanOperator tableScanOperator =
        (QueryExemplarsFunctionTableScanOperator) physicalPlan;
    assertEquals("test_query", tableScanOperator.getRequest().getQuery());
  }

  @Test
  void testCreateScanBuilderWithQueryRangeTableFunction() {
    PrometheusQueryExemplarsRequest exemplarsRequest
        = new PrometheusQueryExemplarsRequest();
    exemplarsRequest.setQuery(QUERY);
    exemplarsRequest.setStartTime(STARTTIME);
    exemplarsRequest.setEndTime(ENDTIME);
    QueryExemplarsTable queryExemplarsTable = new QueryExemplarsTable(client, exemplarsRequest);
    TableScanBuilder tableScanBuilder = queryExemplarsTable.createScanBuilder();
    Assertions.assertNotNull(tableScanBuilder);
    Assertions.assertTrue(tableScanBuilder instanceof QueryExemplarsFunctionTableScanBuilder);
  }

}
