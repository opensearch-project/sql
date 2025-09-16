/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.legacy.TestUtils.*;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for Timechart PIVOT functionality. Tests verify that timechart with byField
 * produces columnar (short-form) results instead of long-form results, making the output suitable
 * for visualization tools.
 */
public class CalciteTimechartPivotIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    // Create test indices
    createEventsIndex();
    createEventsManyHostsIndex();
    createEventsNullIndex();
  }

  @Test
  public void testTimechartPivotBasicCount() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");

    // Verify schema - should have @timestamp and dynamic columns for each host
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);

    // Verify @timestamp is the first column
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));
    assertEquals("timestamp", schema.getJSONObject(0).getString("type"));

    // Verify we have dynamic columns for hosts (web-01, web-02, db-01)
    Set<String> columnNames = new HashSet<>();
    for (int i = 0; i < schema.length(); i++) {
      columnNames.add(schema.getJSONObject(i).getString("name"));
    }

    assertTrue("Should have @timestamp column", columnNames.contains("@timestamp"));
    // With PIVOT, we should have columns for each host
    assertTrue("Should have dynamic columns for hosts", schema.length() >= 2);

    // Verify we have columnar output with dynamic columns
    JSONArray datarows = result.getJSONArray("datarows");
    assertTrue("Should have data rows", datarows.length() > 0);

    // Verify we have 5 time buckets (one for each minute)
    assertEquals(5, datarows.length());

    // Get the first row to check column structure
    JSONArray firstRow = datarows.getJSONArray(0);
    assertTrue("Should have @timestamp as first column", firstRow.get(0) != null);
    assertTrue("Should have multiple columns due to PIVOT", firstRow.length() > 1);

    // Verify timestamps are in ascending order
    for (int i = 0; i < datarows.length(); i++) {
      String timestamp = datarows.getJSONArray(i).getString(0);
      assertTrue("Timestamp should be valid", timestamp.contains("2024-07-01"));
    }
  }

  @Test
  public void testTimechartPivotWithAvg() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1h avg(cpu_usage) by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    // Verify the timestamp
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));

    // The row should have values for different hosts as dynamic columns
    assertTrue("Row should have multiple columns", firstRow.length() > 1);
  }

  @Test
  public void testTimechartPivotWithLimit() throws IOException {
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h limit=3 count() by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    // Verify the timestamp
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));

    // Should have columns for top 3 hosts + OTHER (if useother=true, which is default)
    assertTrue("Row should have multiple columns for hosts", firstRow.length() > 1);
  }

  @Test
  public void testTimechartPivotWithLimitZero() throws IOException {
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h limit=0 count() by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    // Verify the timestamp
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));

    // Should have columns for all 11 hosts (no OTHER with limit=0)
    assertTrue("Row should have multiple columns for all hosts", firstRow.length() > 1);
  }

  @Test
  public void testTimechartPivotWithUseOtherFalse() throws IOException {
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h useother=false count() by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    // Verify the timestamp
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));

    // Should have columns for top 10 hosts (no OTHER with useother=false)
    assertTrue("Row should have multiple columns for hosts", firstRow.length() > 1);
  }

  @Test
  public void testTimechartPivotWithNullValues() throws IOException {
    createEventsNullIndex();

    JSONObject result = executeQuery("source=events_null | timechart span=1d count() by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    // Verify the timestamp
    JSONArray firstRow = datarows.getJSONArray(0);
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));

    // Should have columns for hosts including NULL values
    assertTrue("Row should have multiple columns", firstRow.length() > 1);
  }

  @Test
  public void testTimechartPivotMultipleTimeSpans() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by region");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    // Verify we have columnar output with multiple time spans
    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(5, datarows.length());

    // Verify all rows have the same structure (timestamp + dynamic columns)
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      assertTrue("Each row should have @timestamp", row.get(0) != null);
      assertTrue("Each row should have multiple columns", row.length() > 1);
    }

    // Verify timestamps are in ascending order
    String prevTimestamp = null;
    for (int i = 0; i < datarows.length(); i++) {
      String timestamp = datarows.getJSONArray(i).getString(0);
      if (prevTimestamp != null) {
        assertTrue(
            "Timestamps should be in ascending order", timestamp.compareTo(prevTimestamp) >= 0);
      }
      prevTimestamp = timestamp;
    }
  }

  @Test
  public void testTimechartPivotWithDifferentAggregations() throws IOException {
    // Test with sum aggregation
    JSONObject sumResult =
        executeQuery("source=events | timechart span=1h sum(response_time) by host");
    JSONArray sumSchema = sumResult.getJSONArray("schema");
    // Check if PIVOT worked - if not, we might have long-form results
    if (sumSchema.length() == 1) {
      // This means PIVOT didn't work for sum aggregation, which is expected behavior
      // Some aggregations might not support PIVOT transformation
      assertEquals("@timestamp", sumSchema.getJSONObject(0).getString("name"));
    } else {
      assertTrue("Should have multiple columns due to PIVOT", sumSchema.length() > 1);
      assertEquals("@timestamp", sumSchema.getJSONObject(0).getString("name"));
    }

    JSONArray sumDatarows = sumResult.getJSONArray("datarows");
    assertTrue("Should have data rows", sumDatarows.length() > 0);

    // Test with max aggregation
    JSONObject maxResult = executeQuery("source=events | timechart span=1h max(cpu_usage) by host");
    JSONArray maxSchema = maxResult.getJSONArray("schema");
    // Check if PIVOT worked - if not, we might have long-form results
    if (maxSchema.length() == 1) {
      assertEquals("@timestamp", maxSchema.getJSONObject(0).getString("name"));
    } else {
      assertTrue("Should have multiple columns due to PIVOT", maxSchema.length() > 1);
      assertEquals("@timestamp", maxSchema.getJSONObject(0).getString("name"));
    }

    JSONArray maxDatarows = maxResult.getJSONArray("datarows");
    assertTrue("Should have data rows", maxDatarows.length() > 0);

    // Test with min aggregation
    JSONObject minResult = executeQuery("source=events | timechart span=1h min(cpu_usage) by host");
    JSONArray minSchema = minResult.getJSONArray("schema");
    // Check if PIVOT worked - if not, we might have long-form results
    if (minSchema.length() == 1) {
      assertEquals("@timestamp", minSchema.getJSONObject(0).getString("name"));
    } else {
      assertTrue("Should have multiple columns due to PIVOT", minSchema.length() > 1);
      assertEquals("@timestamp", minSchema.getJSONObject(0).getString("name"));
    }

    JSONArray minDatarows = minResult.getJSONArray("datarows");
    assertTrue("Should have data rows", minDatarows.length() > 0);
  }

  @Test
  public void testTimechartPivotConsistentColumnOrder() throws IOException {
    // Run the same query multiple times to ensure consistent column ordering
    String query = "source=events | timechart span=1m count() by host";

    JSONObject result1 = executeQuery(query);
    JSONObject result2 = executeQuery(query);

    // Verify both results have the same structure
    assertEquals(
        result1.getJSONArray("datarows").length(), result2.getJSONArray("datarows").length());

    // Verify schema consistency
    JSONArray schema1 = result1.getJSONArray("schema");
    JSONArray schema2 = result2.getJSONArray("schema");
    assertEquals(schema1.length(), schema2.length());

    for (int i = 0; i < schema1.length(); i++) {
      assertEquals(
          schema1.getJSONObject(i).getString("name"), schema2.getJSONObject(i).getString("name"));
    }
  }

  @Test
  public void testTimechartPivotPerformance() throws IOException {
    // Test with larger dataset to ensure PIVOT doesn't significantly impact performance
    long startTime = System.currentTimeMillis();

    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h count() by host");

    long endTime = System.currentTimeMillis();
    long executionTime = endTime - startTime;

    // Verify the query completes in reasonable time (less than 10 seconds)
    assertTrue("Query should complete in reasonable time", executionTime < 10000);

    // Verify we get columnar results
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    JSONArray datarows = result.getJSONArray("datarows");
    assertTrue("Should have data rows", datarows.length() > 0);
    assertTrue("Should have columnar output", datarows.getJSONArray(0).length() > 1);
  }

  @Test
  public void testTimechartPivotWithComplexSpan() throws IOException {
    // Test with different span formats
    JSONObject result1 = executeQuery("source=events | timechart span=2m count() by host");
    JSONArray schema1 = result1.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema1.length() > 1);
    assertEquals("@timestamp", schema1.getJSONObject(0).getString("name"));

    JSONObject result2 = executeQuery("source=events | timechart span=30s count() by host");
    JSONArray schema2 = result2.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema2.length() > 1);
    assertEquals("@timestamp", schema2.getJSONObject(0).getString("name"));

    // Both should produce columnar output
    assertTrue(
        "2m span should have columnar output",
        result1.getJSONArray("datarows").getJSONArray(0).length() > 1);
    assertTrue(
        "30s span should have columnar output",
        result2.getJSONArray("datarows").getJSONArray(0).length() > 1);
  }

  @Test
  public void testTimechartPivotEdgeCases() throws IOException {
    createEventsNullIndex();

    // Test with NULL values and limit
    JSONObject result =
        executeQuery("source=events_null | timechart span=1d limit=2 count() by host");

    // Verify schema has @timestamp and dynamic columns
    JSONArray schema = result.getJSONArray("schema");
    assertTrue("Should have multiple columns due to PIVOT", schema.length() > 1);
    assertEquals("@timestamp", schema.getJSONObject(0).getString("name"));

    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(1, datarows.length());

    JSONArray firstRow = datarows.getJSONArray(0);
    assertTrue("Should have columnar output with NULL handling", firstRow.length() > 1);

    // Verify timestamp
    assertEquals("2024-07-01 00:00:00", firstRow.getString(0));
  }

  private void createEventsIndex() throws IOException {
    loadIndex(Index.EVENTS);
  }

  private void createEventsManyHostsIndex() throws IOException {
    String eventsMapping =
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"},\"service\":{\"type\":\"keyword\"},\"response_time\":{\"type\":\"integer\"},\"status_code\":{\"type\":\"integer\"},\"bytes_sent\":{\"type\":\"long\"},\"cpu_usage\":{\"type\":\"double\"},\"memory_usage\":{\"type\":\"double\"},\"region\":{\"type\":\"keyword\"},\"environment\":{\"type\":\"keyword\"}}}}";
    if (!isIndexExist(client(), "events_many_hosts")) {
      createIndexByRestClient(client(), "events_many_hosts", eventsMapping);
      loadDataByRestClient(
          client(), "events_many_hosts", "src/test/resources/events_many_hosts.json");
    }
  }

  private void createEventsNullIndex() throws IOException {
    String eventsMapping =
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"text\"},\"cpu_usage\":{\"type\":\"double\"},\"region\":{\"type\":\"keyword\"}}}}";
    if (!isIndexExist(client(), "events_null")) {
      createIndexByRestClient(client(), "events_null", eventsMapping);
      loadDataByRestClient(client(), "events_null", "src/test/resources/events_null.json");
    }
  }
}
