/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;

@ExtendWith(MockitoExtension.class)
public class SparkSqlFunctionTableScanOperatorTest {

  @Mock
  private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class, sparkSqlFunctionTableScanOperator::open);
    assertEquals("Error fetching data from spark server: Error Message",
        runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testClose() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);
    sparkSqlFunctionTableScanOperator.close();
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    Assertions.assertEquals("sql(select 1)",
        sparkSqlFunctionTableScanOperator.explain());
  }

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject("{\"data\":{"
            + "\"result\":[\"{'1':1}\"],"
            + "\"schema\":[\"{'column_name':'1','data_type':'integer'}\"],"
            + "\"stepId\":\"s-02952063MI629IEUP2P8\","
            + "\"applicationId\":\"application-abc\"}}"));
    sparkSqlFunctionTableScanOperator.open();
    Assertions.assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put("1", new ExprIntegerValue(1));
      }
    });
    assertEquals(firstRow, sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQuerySchema() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(
            new JSONObject(
                "{\"data\":{\"result\":[\"{'1':1}\"],"
                    + "\"schema\":[\"{'column_name':'1','data_type':'integer'}\"],"
                    + "\"stepId\":\"s-02952063MI629IEUP2P8\","
                    + "\"applicationId\":\"application-abc\"}}"));
    sparkSqlFunctionTableScanOperator.open();
    ArrayList<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    columns.add(new ExecutionEngine.Schema.Column("1", "1", ExprCoreType.INTEGER));
    ExecutionEngine.Schema expectedSchema = new ExecutionEngine.Schema(columns);
    assertEquals(expectedSchema, sparkSqlFunctionTableScanOperator.schema());
  }
}
