/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

public class SparkSqlFunctionTableScanBuilderTest {
  @Mock
  private SparkClient sparkClient;

  @Mock
  private LogicalProject logicalProject;

  @Test
  void testBuild() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanBuilder sparkSqlFunctionTableScanBuilder
        = new SparkSqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
    TableScanOperator sqlFunctionTableScanOperator
        = sparkSqlFunctionTableScanBuilder.build();
    Assertions.assertNull(sqlFunctionTableScanOperator);
  }

  @Test
  void testPushProject() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanBuilder sparkSqlFunctionTableScanBuilder
        = new SparkSqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
    Assertions.assertTrue(sparkSqlFunctionTableScanBuilder.pushDownProject(logicalProject));
  }
}
