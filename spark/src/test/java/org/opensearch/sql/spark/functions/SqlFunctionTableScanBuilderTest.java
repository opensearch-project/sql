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
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

public class SqlFunctionTableScanBuilderTest {
  @Mock
  private SparkClient sparkClient;

  @Mock
  private LogicalProject logicalProject;

  @Test
  void testBuild() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanBuilder sqlFunctionTableScanBuilder
        = new SqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
    TableScanOperator sqlFunctionTableScanOperator
        = sqlFunctionTableScanBuilder.build();
    Assertions.assertTrue(sqlFunctionTableScanOperator
        instanceof SqlFunctionTableScanOperator);
  }

  @Test
  void testPushProject() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanBuilder sqlFunctionTableScanBuilder
        = new SqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
    Assertions.assertTrue(sqlFunctionTableScanBuilder.pushDownProject(logicalProject));
  }
}
