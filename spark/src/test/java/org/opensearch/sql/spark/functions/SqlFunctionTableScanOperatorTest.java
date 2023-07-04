/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SqlFunctionTableScanOperatorTest {

  @Mock
  private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanOperator sqlFunctionTableScanOperator
        = new SqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, sqlFunctionTableScanOperator::open);
    assertEquals("Error fetching data from spark server: Error Message",
        runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testClose() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanOperator sqlFunctionTableScanOperator
        = new SqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);
    sqlFunctionTableScanOperator.close();
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanOperator sqlFunctionTableScanOperator
        = new SqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    Assertions.assertEquals("sql(select 1)",
        sqlFunctionTableScanOperator.explain());
  }

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanOperator sqlFunctionTableScanOperator
        = new SqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject("{\"data\":{\"result\":[\"{'1':1}\"],\"schema\":[\"{'column_name':'1','data_type':'integer'}\"],\"stepId\":\"s-02952063MI629IEUP2P8\",\"applicationId\":\"application-abc\"}}"));
    sqlFunctionTableScanOperator.open();
    Assertions.assertTrue(sqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
      put("1", new ExprIntegerValue(1));
    }});
    assertEquals(firstRow, sqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQuerySchema() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SqlFunctionTableScanOperator sqlFunctionTableScanOperator
        = new SqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject("{\"data\":{\"result\":[\"{'1':1}\"],\"schema\":[\"{'column_name':'1','data_type':'integer'}\"],\"stepId\":\"s-02952063MI629IEUP2P8\",\"applicationId\":\"application-abc\"}}"));
    sqlFunctionTableScanOperator.open();
    ArrayList<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    columns.add(new ExecutionEngine.Schema.Column("1", "1", ExprCoreType.INTEGER));
    ExecutionEngine.Schema expectedSchema = new ExecutionEngine.Schema(columns);
    assertEquals(expectedSchema, sqlFunctionTableScanOperator.schema());
  }
}
