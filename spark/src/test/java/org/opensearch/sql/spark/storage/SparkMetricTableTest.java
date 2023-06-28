/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
public class SparkMetricTableTest {
  @Mock
  private SparkClient client;

  @Test
  void testUnsupportedOperation() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    SparkMetricTable sparkMetricTable =
        new SparkMetricTable(client, sparkQueryRequest);

    assertThrows(UnsupportedOperationException.class, sparkMetricTable::exists);
    assertThrows(UnsupportedOperationException.class,
        () -> sparkMetricTable.create(Collections.emptyMap()));
  }

  @Test
  void testCreateScanBuilderWithSqlTableFunction() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");
    SparkMetricTable sparkMetricTable =
        new SparkMetricTable(client, sparkQueryRequest);
    TableScanBuilder tableScanBuilder = sparkMetricTable.createScanBuilder();
    Assertions.assertNotNull(tableScanBuilder);
    Assertions.assertTrue(tableScanBuilder instanceof SqlFunctionTableScanBuilder);
  }

  @Test
  @SneakyThrows
  void testGetFieldTypesFromSparkQueryRequest() {
    SparkMetricTable sparkMetricTable
        = new SparkMetricTable(client, new SparkQueryRequest());
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    Map<String, ExprType> fieldTypes = sparkMetricTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verifyNoMoreInteractions(client);
    assertNotNull(sparkMetricTable.getSparkQueryRequest());
  }

  @Test
  void testImplementWithSqlFunction() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");
    SparkMetricTable sparkMetricTable =
        new SparkMetricTable(client, sparkQueryRequest);
    PhysicalPlan plan = sparkMetricTable.implement(
        new SqlFunctionTableScanBuilder(client, sparkQueryRequest));
    assertTrue(plan instanceof SqlFunctionTableScanOperator);
  }
}
