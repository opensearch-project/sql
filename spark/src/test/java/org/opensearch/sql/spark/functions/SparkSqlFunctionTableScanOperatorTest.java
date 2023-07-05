/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.utils.TestUtils.getJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
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
    assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
        put("1", new ExprIntegerValue(1));
      }
    });
    assertEquals(firstRow, sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testEmptyDataWithException() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject("{\"content\":{"
            + "\"result\":[\"{'name':'Tom'}\"],"
            + "\"schema\":[\"{'column_name':'name','data_type':'string'}\"],"
            + "\"stepId\":\"s-02952063MI629IEUP2P8\","
            + "\"applicationId\":\"application-abc\"}}"));
    RuntimeException exception = assertThrows(RuntimeException.class, sparkSqlFunctionTableScanOperator::open);
    assertEquals("Unexpected result during spark sql query execution", exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testQueryResponseAllTypes() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject(getJson("all_data_type.json")));
    sparkSqlFunctionTableScanOperator.open();
    assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow = new ExprTupleValue(new LinkedHashMap<>() {{
      put("boolean", new ExprBooleanValue(true));
      put("long", new ExprLongValue(922337203));
      put("integer", new ExprIntegerValue(2147483647));
      put("short", new ExprShortValue(32767));
      put("byte", new ExprByteValue(127));
      put("double", new ExprDoubleValue(9223372036854.775807));
      put("float", new ExprFloatValue(21474.83647));
      put("timestamp", new ExprDateValue("2023-07-01 10:31:30"));
      put("date", new ExprTimestampValue("2023-07-01 10:31:30"));
      put("string", new ExprStringValue("ABC"));
      put("char", new ExprStringValue("A"));
    }
    });
    assertEquals(firstRow, sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQueryResponseInvalidDataType() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql("select 1");

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator
        = new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(new JSONObject(getJson("invalid_data_type.json")));

    RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
        () -> sparkSqlFunctionTableScanOperator.open());
    Assertions.assertEquals("Result contains invalid data type",
        exception.getMessage());
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
