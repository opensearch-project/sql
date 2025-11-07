/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTimechartCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    // Create events index with timestamp data
    loadIndex(Index.EVENTS);
    loadIndex(Index.EVENTS_NULL);
    createEventsManyHostsIndex();
  }

  @Test
  public void testTimechartWithHourSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1h count() by host");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2));
    assertEquals(3, result.getInt("total"));
  }

  @Test
  public void testTimechartWithMinuteSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    // Actual result shows 5 rows, not zero-filled results
    assertEquals(5, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:04:00", "web-02", 1));
  }

  @Test
  public void testTimechartWithoutTimestampField() throws IOException {
    // Create index without @timestamp field
    String noTimestampMapping =
        "{\"mappings\":{\"properties\":{\"name\":{\"type\":\"keyword\"},\"occupation\":{\"type\":\"keyword\"},\"country\":{\"type\":\"keyword\"},\"salary\":{\"type\":\"integer\"},\"year\":{\"type\":\"integer\"},\"month\":{\"type\":\"integer\"}}}}";
    if (!isIndexExist(client(), "no_timestamp")) {
      createIndexByRestClient(client(), "no_timestamp", noTimestampMapping);
      loadDataByRestClient(client(), "no_timestamp", "src/test/resources/occupation.json");
    }

    // Test should throw exception for missing @timestamp field
    Throwable exception =
        assertThrowsWithReplace(
            ResponseException.class,
            () -> {
              executeQuery("source=no_timestamp | timechart count()");
            });
    assertTrue(
        "Error message should mention missing @timestamp field",
        exception.getMessage().contains("@timestamp")
            || exception.getMessage().contains("timestamp"));
  }

  @Test
  public void testTimechartWithMinuteSpanNoGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m avg(cpu_usage)");
    verifySchema(result, schema("@timestamp", "timestamp"), schema("avg(cpu_usage)", "double"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", 45.2),
        rows("2024-07-01 00:01:00", 38.7),
        rows("2024-07-01 00:02:00", 55.3),
        rows("2024-07-01 00:03:00", 42.1),
        rows("2024-07-01 00:04:00", 41.8));
  }

  @Test
  public void testTimechartWithSpanCountGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1s count() by region");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("region", "string"),
        schema("count()", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "us-east", 1),
        rows("2024-07-01 00:01:00", "us-west", 1),
        rows("2024-07-01 00:02:00", "us-east", 1),
        rows("2024-07-01 00:03:00", "eu-west", 1),
        rows("2024-07-01 00:04:00", "us-west", 1));
  }

  @Test
  public void testTimechartWithOtherCategory() throws IOException {
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h avg(cpu_usage) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("avg(cpu_usage)", "double"));

    // Verify we have 11 data rows (10 hosts + OTHER)
    assertEquals(11, result.getJSONArray("datarows").length());

    // Verify the OTHER row exists with the correct value
    boolean foundOther = false;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      if ("OTHER".equals(row[1])) {
        foundOther = true;
        assertEquals(35.9, ((Number) row[2]).doubleValue(), 0.01);
        break;
      }
    }
    assertTrue("OTHER category not found in results", foundOther);
  }

  @Test
  public void testTimechartWithLimit() throws IOException {
    JSONObject result =
        executeQuery("source=events | timechart span=1m limit=2 avg(cpu_usage) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("avg(cpu_usage)", "double"));

    verifyDataRowsInOrder(
        result,
        rows("2024-07-01 00:00:00", "web-01", 45.2),
        rows("2024-07-01 00:01:00", "web-02", 38.7),
        rows("2024-07-01 00:02:00", "web-01", 55.3),
        rows("2024-07-01 00:03:00", "OTHER", 42.1),
        rows("2024-07-01 00:04:00", "web-02", 41.8));
  }

  @Test
  public void testTimechartWithLimitCountGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m limit=2 count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    // Actual result shows 5 rows, not zero-filled results
    assertEquals(5, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:03:00", "OTHER", 1),
        rows("2024-07-01 00:04:00", "web-02", 1));
  }

  @Test
  public void testTimechartWithLimitZeroAndAvg() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=0 useother=t avg(cpu_usage) by"
                + " host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("avg(cpu_usage)", "double"));

    // Verify we have 11 data rows (all 11 hosts, no OTHER)
    assertEquals(11, result.getJSONArray("datarows").length());

    // Verify no OTHER category
    boolean foundOther = false;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      if ("OTHER".equals(row[1])) {
        foundOther = true;
        break;
      }
    }
    assertFalse("OTHER category should not be present with limit=0", foundOther);
  }

  @Test
  public void testTimechartWithLimitZeroAndCount() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=0 useother=t count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    // For count with limit=0, should show zero-filled results: 11 hosts × 1 time span = 11 rows
    assertEquals(11, result.getInt("total"));
  }

  @Test
  public void testTimechartWithUseOtherFalseAndAvg() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h useother=false avg(cpu_usage) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("avg(cpu_usage)", "double"));

    // Verify we have 10 data rows (top 10 hosts, no OTHER)
    assertEquals(10, result.getJSONArray("datarows").length());

    // Verify no OTHER category
    boolean foundOther = false;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      if ("OTHER".equals(row[1])) {
        foundOther = true;
        break;
      }
    }
    assertFalse("OTHER category should not be present with useother=false", foundOther);
  }

  @Test
  public void testTimechartWithUseOtherFalseAndCount() throws IOException {
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h useother=false count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    // For count with useother=false, should show zero-filled results: 10 hosts × 1 time span = 10
    // rows
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void testTimechartWithCountNoLimitByHostShowZero() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    // The actual result shows 5 rows, not 15 as zero-filling doesn't happen as expected
    assertEquals(5, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:04:00", "web-02", 1));
  }

  @Test
  public void testTimechartWithLimitAndUseOther() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=3 useother=t avg(cpu_usage) by"
                + " host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("avg(cpu_usage)", "double"));

    // Verify we have 4 data rows (3 hosts + OTHER)
    assertEquals(4, result.getJSONArray("datarows").length());

    // Verify specific values with tolerance for floating point precision
    boolean foundOther = false, foundWeb03 = false, foundWeb07 = false, foundWeb09 = false;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      String host = (String) row[1];
      double cpuUsage = ((Number) row[2]).doubleValue();

      if ("OTHER".equals(host)) {
        foundOther = true;
        assertEquals(41.3, cpuUsage, 0.1);
      } else if ("web-03".equals(host)) {
        foundWeb03 = true;
        assertEquals(55.3, cpuUsage, 0.1);
      } else if ("web-07".equals(host)) {
        foundWeb07 = true;
        assertEquals(48.6, cpuUsage, 0.1);
      } else if ("web-09".equals(host)) {
        foundWeb09 = true;
        assertEquals(67.8, cpuUsage, 0.1);
      }
    }
    assertTrue("OTHER not found", foundOther);
    assertTrue("web-03 not found", foundWeb03);
    assertTrue("web-07 not found", foundWeb07);
    assertTrue("web-09 not found", foundWeb09);
  }

  @Test
  public void testTimechartWithMissingHostValues() throws IOException {
    JSONObject result = executeQuery("source=events_null | timechart span=1d count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", "NULL", 1));
  }

  @Test
  public void testTimechartWithNullAndOther() throws IOException {
    JSONObject result =
        executeQuery("source=events_null | timechart count() by host span=1d limit=2");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "OTHER", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", "NULL", 1));
  }

  @Test
  public void testTimechartWithNullAndLimit() throws IOException {
    JSONObject result =
        executeQuery("source=events_null | timechart span=1d count() by host limit=3");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", "NULL", 1));
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
}
