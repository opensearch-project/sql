/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.functions.scan;


import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.STEP;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

public class  QueryRangeFunctionTableScanBuilderTest {

  @Mock
  private PrometheusClient prometheusClient;

  @Mock
  private LogicalProject logicalProject;

  @Test
  void testBuild() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanBuilder queryRangeFunctionTableScanBuilder
        = new QueryRangeFunctionTableScanBuilder(prometheusClient, prometheusQueryRequest);
    TableScanOperator queryRangeFunctionTableScanOperator
        = queryRangeFunctionTableScanBuilder.build();
    Assertions.assertNotNull(queryRangeFunctionTableScanOperator);
    Assertions.assertTrue(queryRangeFunctionTableScanOperator
        instanceof QueryRangeFunctionTableScanOperator);
  }

  @Test
  void testPushProject() {
    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    prometheusQueryRequest.setPromQl(QUERY);
    prometheusQueryRequest.setStartTime(STARTTIME);
    prometheusQueryRequest.setEndTime(ENDTIME);
    prometheusQueryRequest.setStep(STEP);

    QueryRangeFunctionTableScanBuilder queryRangeFunctionTableScanBuilder
        = new QueryRangeFunctionTableScanBuilder(prometheusClient, prometheusQueryRequest);
    Assertions.assertTrue(queryRangeFunctionTableScanBuilder.pushDownProject(logicalProject));
  }
}
