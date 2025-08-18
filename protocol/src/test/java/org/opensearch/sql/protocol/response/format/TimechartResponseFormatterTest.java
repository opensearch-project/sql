/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.TimechartResponseFormatter.JsonResponse;

class TimechartResponseFormatterTest {

  @Test
  void testSimpleTimechartWithoutByField() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "value"),
            Map.of("timestamp", "timestamp", "value", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", 10.5},
                new Object[] {"2024-01-01 01:00:00", 20.3}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(2, response.getTotal());
    assertEquals(2, response.getSize());
    assertEquals(2, response.getSchema().size());
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("value", response.getSchema().get(1).getName());
    assertEquals(2, response.getDatarows().length);
  }

  @Test
  void testTimechartWithByField() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "web-01", 45.2},
                new Object[] {"2024-01-01 00:00:00", "web-02", 38.7},
                new Object[] {"2024-01-01 01:00:00", "web-01", 52.1}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(2, response.getTotal());
    assertEquals(2, response.getSize());
    assertEquals(3, response.getSchema().size()); // timestamp + web-01 + web-02
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("web-01", response.getSchema().get(1).getName());
    assertEquals("web-02", response.getSchema().get(2).getName());
  }

  @Test
  void testTimechartWithOtherCategory() {
    // Create data with more than 10 distinct values to trigger OTHER category
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "host-01", 10.0},
                new Object[] {"2024-01-01 00:00:00", "host-02", 20.0},
                new Object[] {"2024-01-01 00:00:00", "host-03", 30.0},
                new Object[] {"2024-01-01 00:00:00", "host-04", 40.0},
                new Object[] {"2024-01-01 00:00:00", "host-05", 50.0},
                new Object[] {"2024-01-01 00:00:00", "host-06", 60.0},
                new Object[] {"2024-01-01 00:00:00", "host-07", 70.0},
                new Object[] {"2024-01-01 00:00:00", "host-08", 80.0},
                new Object[] {"2024-01-01 00:00:00", "host-09", 90.0},
                new Object[] {"2024-01-01 00:00:00", "host-10", 100.0},
                new Object[] {"2024-01-01 00:00:00", "host-11", 5.0},
                new Object[] {"2024-01-01 00:00:00", "host-12", 3.0}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(12, response.getSchema().size()); // timestamp + 10 hosts + OTHER
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("OTHER", response.getSchema().get(11).getName());

    // Verify OTHER column has aggregated value
    Object[] dataRow = response.getDatarows()[0];
    Object otherValue = dataRow[11];
    assertNotNull(otherValue);
    assertEquals(8.0, (Double) otherValue, 0.001); // host-11 (5.0) + host-12 (3.0)
  }

  @Test
  void testTimechartWithLimitZero() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "host-01", 10.0},
                new Object[] {"2024-01-01 00:00:00", "host-02", 20.0},
                new Object[] {"2024-01-01 00:00:00", "host-03", 30.0}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 0, true);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(4, response.getSchema().size()); // timestamp + 3 hosts (no OTHER)
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("host-01", response.getSchema().get(1).getName());
    assertEquals("host-02", response.getSchema().get(2).getName());
    assertEquals("host-03", response.getSchema().get(3).getName());
  }

  @Test
  void testTimechartWithUseOtherFalse() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "host-01", 100.0},
                new Object[] {"2024-01-01 00:00:00", "host-02", 90.0},
                new Object[] {"2024-01-01 00:00:00", "host-03", 80.0},
                new Object[] {"2024-01-01 00:00:00", "host-04", 70.0},
                new Object[] {"2024-01-01 00:00:00", "host-05", 60.0}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 3, false);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(4, response.getSchema().size()); // timestamp + top 3 hosts (no OTHER)
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("host-01", response.getSchema().get(1).getName());
    assertEquals("host-02", response.getSchema().get(2).getName());
    assertEquals("host-03", response.getSchema().get(3).getName());
  }

  @Test
  void testTimechartWithLimitAndUseOther() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "host-01", 100.0},
                new Object[] {"2024-01-01 00:00:00", "host-02", 90.0},
                new Object[] {"2024-01-01 00:00:00", "host-03", 80.0},
                new Object[] {"2024-01-01 00:00:00", "host-04", 70.0},
                new Object[] {"2024-01-01 00:00:00", "host-05", 60.0}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 3, true);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(5, response.getSchema().size()); // timestamp + top 3 hosts + OTHER
    assertEquals("timestamp", response.getSchema().get(0).getName());
    assertEquals("host-01", response.getSchema().get(1).getName());
    assertEquals("host-02", response.getSchema().get(2).getName());
    assertEquals("host-03", response.getSchema().get(3).getName());
    assertEquals("OTHER", response.getSchema().get(4).getName());

    // Verify OTHER column has aggregated value
    Object[] dataRow = response.getDatarows()[0];
    assertEquals(130.0, (Double) dataRow[4], 0.001); // host-04 (70.0) + host-05 (60.0)
  }

  @Test
  void testConvertToDoubleWithNullValue() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "web-01", null},
                new Object[] {"2024-01-01 00:00:00", "web-02", 38.7}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    Object[] dataRow = response.getDatarows()[0];
    assertEquals(null, dataRow[1]); // null value should remain null
    assertEquals(38.7, dataRow[2]);
  }

  @Test
  void testTimechartWithMoreThanThreeColumns() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage", "memory_usage"),
            Map.of(
                "timestamp",
                "timestamp",
                "host",
                "keyword",
                "cpu_usage",
                "double",
                "memory_usage",
                "double"),
            Arrays.<Object[]>asList(new Object[] {"2024-01-01 00:00:00", "web-01", 45.2, 67.8}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    // Should fall back to simple JSON format
    assertEquals(1, response.getTotal());
    assertEquals(4, response.getSchema().size());
  }

  @Test
  void testConstructorWithNullParameters() {
    TimechartResponseFormatter formatter1 =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, null, null);

    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "value"),
            Map.of("timestamp", "timestamp", "value", "double"),
            Arrays.<Object[]>asList(new Object[] {"2024-01-01 00:00:00", 10.5}));

    JsonResponse response = (JsonResponse) formatter1.buildJsonObject(queryResult);
    assertNotNull(response);
    assertEquals(1, response.getTotal());
  }

  @Test
  void testConstructorWithMaxDistinctValues() {
    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 5);

    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "value"),
            Map.of("timestamp", "timestamp", "value", "double"),
            Arrays.<Object[]>asList(new Object[] {"2024-01-01 00:00:00", 10.5}));

    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);
    assertNotNull(response);
    assertEquals(1, response.getTotal());
  }

  @Test
  void testConvertToDoubleWithStringValue() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "web-01", "45.5"},
                new Object[] {"2024-01-01 00:00:00", "web-02", "invalid"}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertNotNull(response.getDatarows());
  }

  @Test
  void testJsonResponseBuilder() {
    TimechartResponseFormatter.JsonResponse response =
        TimechartResponseFormatter.JsonResponse.builder()
            .column(new TimechartResponseFormatter.Column("test", "string"))
            .datarows(new Object[][] {{"value"}})
            .total(1)
            .size(1)
            .build();

    assertEquals(1, response.getTotal());
    assertEquals(1, response.getSize());
    assertEquals(1, response.getSchema().size());
    assertEquals("test", response.getSchema().get(0).getName());
    assertEquals("string", response.getSchema().get(0).getType());
  }

  @Test
  void testColumnClass() {
    TimechartResponseFormatter.Column column =
        new TimechartResponseFormatter.Column("testName", "testType");
    assertEquals("testName", column.getName());
    assertEquals("testType", column.getType());
  }

  @Test
  void testConvertToDoubleEdgeCases() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "web-01", "not-a-number"},
                new Object[] {"2024-01-01 00:00:00", "web-02", 42}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 1, true);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(3, response.getSchema().size()); // timestamp + web-02 + OTHER
    Object[] dataRow = response.getDatarows()[0];
    Object otherValue = dataRow[2];
    if (otherValue != null) {
      assertEquals(
          0.0,
          ((Number) otherValue).doubleValue(),
          0.001); // OTHER should be 0.0 for invalid string
    } else {
      // OTHER is null when sum is 0.0, which is expected
      assertNull(otherValue);
    }
  }

  @Test
  void testEmptyQueryResult() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList());

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(0, response.getTotal());
    assertEquals(1, response.getSchema().size()); // Only timestamp column
  }

  @Test
  void testOtherCategoryWithZeroSum() {
    QueryResult queryResult =
        mockQueryResult(
            Arrays.asList("timestamp", "host", "cpu_usage"),
            Map.of("timestamp", "timestamp", "host", "keyword", "cpu_usage", "double"),
            Arrays.<Object[]>asList(
                new Object[] {"2024-01-01 00:00:00", "host-01", 100.0},
                new Object[] {"2024-01-01 00:00:00", "host-02", 90.0},
                new Object[] {"2024-01-01 00:00:00", "host-03", 0.0}));

    TimechartResponseFormatter formatter =
        new TimechartResponseFormatter(JsonResponseFormatter.Style.COMPACT, 2, true);
    JsonResponse response = (JsonResponse) formatter.buildJsonObject(queryResult);

    assertEquals(1, response.getTotal());
    assertEquals(4, response.getSchema().size()); // timestamp + top 2 hosts + OTHER
    Object[] dataRow = response.getDatarows()[0];
    assertEquals(null, dataRow[3]); // OTHER should be null when sum is 0.0
  }

  @SuppressWarnings("unchecked")
  private QueryResult mockQueryResult(
      List<String> columnNames, Map<String, String> columnTypes, List<Object[]> rows) {
    QueryResult queryResult = mock(QueryResult.class);

    // Create LinkedHashMap to preserve order
    LinkedHashMap<String, String> orderedTypes = new LinkedHashMap<>();
    for (String name : columnNames) {
      orderedTypes.put(name, columnTypes.get(name));
    }

    when(queryResult.columnNameTypes()).thenReturn(orderedTypes);
    when(queryResult.size()).thenReturn(rows.size());
    when(queryResult.iterator()).thenReturn((Iterator<Object[]>) rows.iterator());

    return queryResult;
  }
}
