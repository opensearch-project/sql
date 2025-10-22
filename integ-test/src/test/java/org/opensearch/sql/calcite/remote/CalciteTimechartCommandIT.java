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
    createEventsIndex();
    createEventsManyHostsIndex();
    createEventsNullIndex();
  }

  @Test
  public void testTimechartWithHourSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1h count() by host");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));
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
        schema("count", "bigint"));

    // For count aggregation with default limit (no OTHER needed): 3 hosts × 5 time spans = 15 rows
    assertEquals(15, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 0),
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:00:00", "web-02", 0),
        rows("2024-07-01 00:01:00", "db-01", 0),
        rows("2024-07-01 00:01:00", "web-01", 0),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "db-01", 0),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:02:00", "web-02", 0),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:03:00", "web-01", 0),
        rows("2024-07-01 00:03:00", "web-02", 0),
        rows("2024-07-01 00:04:00", "db-01", 0),
        rows("2024-07-01 00:04:00", "web-01", 0),
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
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testTimechartWithSpanCountGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1s count() by region");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("region", "string"),
        schema("count", "bigint"));
    // For count aggregation with 3 regions (< default limit 10), should show zero-filled results: 3
    // regions × 5 time spans = 15 rows
    assertEquals(15, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "us-east", 1),
        rows("2024-07-01 00:00:00", "us-west", 0),
        rows("2024-07-01 00:00:00", "eu-west", 0),
        rows("2024-07-01 00:01:00", "us-east", 0),
        rows("2024-07-01 00:01:00", "us-west", 1),
        rows("2024-07-01 00:01:00", "eu-west", 0),
        rows("2024-07-01 00:02:00", "us-east", 1),
        rows("2024-07-01 00:02:00", "us-west", 0),
        rows("2024-07-01 00:02:00", "eu-west", 0),
        rows("2024-07-01 00:03:00", "us-east", 0),
        rows("2024-07-01 00:03:00", "us-west", 0),
        rows("2024-07-01 00:03:00", "eu-west", 1),
        rows("2024-07-01 00:04:00", "us-east", 0),
        rows("2024-07-01 00:04:00", "us-west", 1),
        rows("2024-07-01 00:04:00", "eu-west", 0));
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

    // Verify we have rows for web-01, web-02, and OTHER
    boolean foundWeb01 = false;
    boolean foundWeb02 = false;
    boolean foundOther = false;

    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      String label = (String) row[1];

      if ("web-01".equals(label)) {
        foundWeb01 = true;
      } else if ("web-02".equals(label)) {
        foundWeb02 = true;
      } else if ("OTHER".equals(label)) {
        foundOther = true;
      }
    }

    assertTrue("web-01 not found in results", foundWeb01);
    assertTrue("web-02 not found in results", foundWeb02);
    assertTrue("OTHER category not found in results", foundOther);
  }

  @Test
  public void testTimechartWithLimitCountGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m limit=2 count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));

    // For count with limit=2, should show zero-filled results: 3 hosts (web-01, web-02, OTHER) × 5
    // time spans = 15 rows
    assertEquals(15, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:00:00", "web-02", 0),
        rows("2024-07-01 00:00:00", "OTHER", 0),
        rows("2024-07-01 00:01:00", "web-01", 0),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:01:00", "OTHER", 0),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:02:00", "web-02", 0),
        rows("2024-07-01 00:02:00", "OTHER", 0),
        rows("2024-07-01 00:03:00", "web-01", 0),
        rows("2024-07-01 00:03:00", "web-02", 0),
        rows("2024-07-01 00:03:00", "OTHER", 1),
        rows("2024-07-01 00:04:00", "web-01", 0),
        rows("2024-07-01 00:04:00", "web-02", 1),
        rows("2024-07-01 00:04:00", "OTHER", 0));
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
        schema("count", "bigint"));

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
        schema("count", "bigint"));

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
        schema("count", "bigint"));

    // For count aggregation, should show zero-filled results: 3 hosts × 5 time spans = 15 rows
    assertEquals(15, result.getInt("total"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:00:00", "web-02", 0),
        rows("2024-07-01 00:00:00", "db-01", 0),
        rows("2024-07-01 00:01:00", "web-01", 0),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:01:00", "db-01", 0),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:02:00", "web-02", 0),
        rows("2024-07-01 00:02:00", "db-01", 0),
        rows("2024-07-01 00:03:00", "web-01", 0),
        rows("2024-07-01 00:03:00", "web-02", 0),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:04:00", "web-01", 0),
        rows("2024-07-01 00:04:00", "web-02", 1),
        rows("2024-07-01 00:04:00", "db-01", 0));
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
        assertEquals(330.4, cpuUsage, 0.1);
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
    createEventsNullIndex();

    JSONObject result = executeQuery("source=events_null | timechart span=1d count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", null, 1));

    assertEquals(4, result.getInt("total"));
  }

  @Test
  public void testTimechartWithNullAndOther() throws IOException {
    createEventsNullIndex();

    JSONObject result =
        executeQuery("source=events_null | timechart span=1d limit=2 count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "OTHER", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", null, 1));

    assertEquals(4, result.getInt("total"));
  }

  @Test
  public void testTimechartWithNullAndLimit() throws IOException {
    createEventsNullIndex();

    JSONObject result =
        executeQuery("source=events_null | timechart span=1d limit=3 count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "db-01", 1),
        rows("2024-07-01 00:00:00", "web-01", 2),
        rows("2024-07-01 00:00:00", "web-02", 2),
        rows("2024-07-01 00:00:00", null, 1));

    assertEquals(4, result.getInt("total"));
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
