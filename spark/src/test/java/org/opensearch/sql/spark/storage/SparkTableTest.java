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
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.read.TableScanBuilder;

@ExtendWith(MockitoExtension.class)
public class SparkTableTest {
  @Mock
  private SparkClient client;

  @Test
  void testUnsupportedOperation() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    SparkTable sparkTable =
        new SparkTable(client, sparkQueryRequest);

    assertThrows(UnsupportedOperationException.class, sparkTable::exists);
    assertThrows(UnsupportedOperationException.class,
        () -> sparkTable.create(Collections.emptyMap()));
  }

  @Test
  void testCreateScanBuilderWithSqlTableFunction() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");
    SparkTable sparkTable =
        new SparkTable(client, sparkQueryRequest);
    TableScanBuilder tableScanBuilder = sparkTable.createScanBuilder();
    Assertions.assertNotNull(tableScanBuilder);
    Assertions.assertTrue(tableScanBuilder instanceof SparkSqlFunctionTableScanBuilder);
  }

  @Test
  @SneakyThrows
  void testGetFieldTypesFromSparkQueryRequest() {
    SparkTable sparkTable
        = new SparkTable(client, new SparkQueryRequest());
    Map<String, ExprType> expectedFieldTypes = new HashMap<>();
    Map<String, ExprType> fieldTypes = sparkTable.getFieldTypes();

    assertEquals(expectedFieldTypes, fieldTypes);
    verifyNoMoreInteractions(client);
    assertNotNull(sparkTable.getSparkQueryRequest());
  }

  @Test
  void testImplementWithSqlFunction() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");
    SparkTable sparkMetricTable =
        new SparkTable(client, sparkQueryRequest);
    PhysicalPlan plan = sparkMetricTable.implement(
        new SparkSqlFunctionTableScanBuilder(client, sparkQueryRequest));
    assertTrue(plan instanceof SparkSqlFunctionTableScanOperator);
  }
}
