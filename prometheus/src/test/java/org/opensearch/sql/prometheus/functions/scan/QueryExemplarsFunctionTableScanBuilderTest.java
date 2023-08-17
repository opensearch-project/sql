/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.functions.scan;

import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryExemplarsRequest;
import org.opensearch.sql.storage.TableScanOperator;

public class QueryExemplarsFunctionTableScanBuilderTest {

  @Mock private PrometheusClient prometheusClient;

  @Mock private LogicalProject logicalProject;

  @Test
  void testBuild() {
    PrometheusQueryExemplarsRequest exemplarsRequest = new PrometheusQueryExemplarsRequest();
    exemplarsRequest.setQuery(QUERY);
    exemplarsRequest.setStartTime(STARTTIME);
    exemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanBuilder queryExemplarsFunctionTableScanBuilder =
        new QueryExemplarsFunctionTableScanBuilder(prometheusClient, exemplarsRequest);
    TableScanOperator queryExemplarsFunctionTableScanOperator =
        queryExemplarsFunctionTableScanBuilder.build();
    Assertions.assertNotNull(queryExemplarsFunctionTableScanOperator);
    Assertions.assertTrue(
        queryExemplarsFunctionTableScanOperator instanceof QueryExemplarsFunctionTableScanOperator);
  }

  @Test
  void testPushProject() {
    PrometheusQueryExemplarsRequest exemplarsRequest = new PrometheusQueryExemplarsRequest();
    exemplarsRequest.setQuery(QUERY);
    exemplarsRequest.setStartTime(STARTTIME);
    exemplarsRequest.setEndTime(ENDTIME);

    QueryExemplarsFunctionTableScanBuilder queryExemplarsFunctionTableScanBuilder =
        new QueryExemplarsFunctionTableScanBuilder(prometheusClient, exemplarsRequest);
    Assertions.assertTrue(queryExemplarsFunctionTableScanBuilder.pushDownProject(logicalProject));
  }
}
