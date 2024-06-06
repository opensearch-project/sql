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
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;
import static org.opensearch.sql.spark.utils.TestUtils.getJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import lombok.SneakyThrows;
import org.json.JSONArray;
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
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.data.type.SparkDataType;
import org.opensearch.sql.spark.data.value.SparkExprValue;
import org.opensearch.sql.spark.functions.scan.SparkSqlFunctionTableScanOperator;
import org.opensearch.sql.spark.request.SparkQueryRequest;

@ExtendWith(MockitoExtension.class)
public class SparkSqlFunctionTableScanOperatorTest {

  @Mock private SparkClient sparkClient;

  @Test
  @SneakyThrows
  void testEmptyQueryWithException() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenThrow(new IOException("Error Message"));
    RuntimeException runtimeException =
        assertThrows(RuntimeException.class, sparkSqlFunctionTableScanOperator::open);
    assertEquals(
        "Error fetching data from spark server: Error Message", runtimeException.getMessage());
  }

  @Test
  @SneakyThrows
  void testClose() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);
    sparkSqlFunctionTableScanOperator.close();
  }

  @Test
  @SneakyThrows
  void testExplain() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    Assertions.assertEquals("sql(select 1)", sparkSqlFunctionTableScanOperator.explain());
  }

  @Test
  @SneakyThrows
  void testQueryResponseIterator() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenReturn(new JSONObject(getJson("select_query_response.json")));
    sparkSqlFunctionTableScanOperator.open();
    assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow =
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("1", new ExprIntegerValue(1));
              }
            });
    assertEquals(firstRow, sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQueryResponseAllTypes() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenReturn(new JSONObject(getJson("all_data_type.json")));
    sparkSqlFunctionTableScanOperator.open();
    assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    ExprTupleValue firstRow =
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("boolean", ExprBooleanValue.of(true));
                put("long", new ExprLongValue(922337203));
                put("integer", new ExprIntegerValue(2147483647));
                put("short", new ExprShortValue(32767));
                put("byte", new ExprByteValue(127));
                put("double", new ExprDoubleValue(9223372036854.775807));
                put("float", new ExprFloatValue(21474.83647));
                put("timestamp", new ExprDateValue("2023-07-01 10:31:30"));
                put("date", new ExprTimestampValue("2023-07-01 10:31:30"));
                put("string", new ExprStringValue("ABC"));
                put("char", new SparkExprValue(new SparkDataType("char"), "A"));
              }
            });
    assertEquals(firstRow, sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  void testQueryResponseSparkDataType() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenReturn(new JSONObject(getJson("spark_data_type.json")));
    sparkSqlFunctionTableScanOperator.open();
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put(
                    "struct_column",
                    new SparkExprValue(
                        new SparkDataType("struct"),
                        new JSONObject("{\"struct_value\":\"value\"}}").toMap()));
                put(
                    "array_column",
                    new SparkExprValue(
                        new SparkDataType("array"), new JSONArray("[1,2]").toList()));
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
  }

  @Test
  @SneakyThrows
  void testQuerySchema() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenReturn(new JSONObject(getJson("select_query_response.json")));
    sparkSqlFunctionTableScanOperator.open();
    ArrayList<ExecutionEngine.Schema.Column> columns = new ArrayList<>();
    columns.add(new ExecutionEngine.Schema.Column("1", "1", ExprCoreType.INTEGER));
    ExecutionEngine.Schema expectedSchema = new ExecutionEngine.Schema(columns);
    assertEquals(expectedSchema, sparkSqlFunctionTableScanOperator.schema());
  }

  /** https://github.com/opensearch-project/sql/issues/2210. */
  @Test
  @SneakyThrows
  void issue2210() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any())).thenReturn(new JSONObject(getJson("issue2210.json")));
    sparkSqlFunctionTableScanOperator.open();
    assertTrue(sparkSqlFunctionTableScanOperator.hasNext());
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("col_name", stringValue("day"));
                put("data_type", stringValue("int"));
                put("comment", nullValue());
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("col_name", stringValue("# Partition Information"));
                put("data_type", stringValue(""));
                put("comment", stringValue(""));
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("col_name", stringValue("# col_name"));
                put("data_type", stringValue("data_type"));
                put("comment", stringValue("comment"));
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("col_name", stringValue("day"));
                put("data_type", stringValue("int"));
                put("comment", nullValue());
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
    Assertions.assertFalse(sparkSqlFunctionTableScanOperator.hasNext());
  }

  @Test
  @SneakyThrows
  public void issue2367MissingFields() {
    SparkQueryRequest sparkQueryRequest = new SparkQueryRequest();
    sparkQueryRequest.setSql(QUERY);

    SparkSqlFunctionTableScanOperator sparkSqlFunctionTableScanOperator =
        new SparkSqlFunctionTableScanOperator(sparkClient, sparkQueryRequest);

    when(sparkClient.sql(any()))
        .thenReturn(
            new JSONObject(
                "{\n"
                    + "  \"data\": {\n"
                    + "    \"result\": [\n"
                    + "      \"{}\",\n"
                    + "      \"{'srcPort':20641}\"\n"
                    + "    ],\n"
                    + "    \"schema\": [\n"
                    + "      \"{'column_name':'srcPort','data_type':'long'}\"\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}"));
    sparkSqlFunctionTableScanOperator.open();
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("srcPort", ExprNullValue.of());
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
    assertEquals(
        new ExprTupleValue(
            new LinkedHashMap<>() {
              {
                put("srcPort", new ExprLongValue(20641L));
              }
            }),
        sparkSqlFunctionTableScanOperator.next());
  }
}
