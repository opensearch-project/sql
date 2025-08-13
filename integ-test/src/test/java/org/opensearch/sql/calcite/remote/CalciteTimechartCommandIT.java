/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

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
    String eventsMapping =
        "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"},\"service\":{\"type\":\"keyword\"},\"response_time\":{\"type\":\"integer\"},\"status_code\":{\"type\":\"integer\"},\"bytes_sent\":{\"type\":\"long\"},\"cpu_usage\":{\"type\":\"double\"},\"memory_usage\":{\"type\":\"double\"},\"region\":{\"type\":\"keyword\"},\"environment\":{\"type\":\"keyword\"}}}}";
    if (!isIndexExist(client(), "events")) {
      createIndexByRestClient(client(), "events", eventsMapping);
      loadDataByRestClient(client(), "events", "src/test/resources/events_test.json");
    }

    // Create events_many_hosts index with many distinct host values
    if (!isIndexExist(client(), "events_many_hosts")) {
      createIndexByRestClient(client(), "events_many_hosts", eventsMapping);
      loadDataByRestClient(
          client(), "events_many_hosts", "src/test/resources/events_many_hosts.json");
    }
  }

  @Test
  public void testTimechartWithHourSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1h count() by host");
    verifySchema(
        result,
        schema("$f2", "timestamp"),
        schema("cache-01", "bigint"),
        schema("cache-02", "bigint"),
        schema("db-01", "bigint"),
        schema("db-02", "bigint"),
        schema("lb-01", "bigint"),
        schema("web-01", "bigint"),
        schema("web-02", "bigint"),
        schema("web-03", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:00:00", 1, 1, 1, 1, 1, 6, 5, 5));
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithMinuteSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");
    verifySchema(
        result,
        schema("$f2", "timestamp"),
        schema("cache-01", "bigint"),
        schema("cache-02", "bigint"),
        schema("db-01", "bigint"),
        schema("db-02", "bigint"),
        schema("lb-01", "bigint"),
        schema("web-01", "bigint"),
        schema("web-02", "bigint"),
        schema("web-03", "bigint"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:01:00", null, null, null, null, null, null, 1, null),
        rows("2024-07-01 00:02:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:03:00", null, null, null, null, null, null, null, 1),
        rows("2024-07-01 00:04:00", null, null, null, null, null, null, 1, null),
        rows("2024-07-01 00:05:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:06:00", null, null, null, null, null, null, null, 1),
        rows("2024-07-01 00:07:00", null, null, null, null, null, null, 1, null),
        rows("2024-07-01 00:08:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:09:00", null, null, null, null, null, null, null, 1),
        rows("2024-07-01 00:10:00", null, null, null, null, null, null, 1, null),
        rows("2024-07-01 00:11:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:12:00", null, null, null, null, null, null, null, 1),
        rows("2024-07-01 00:13:00", null, null, null, null, null, null, 1, null),
        rows("2024-07-01 00:14:00", null, null, null, null, null, 1, null, null),
        rows("2024-07-01 00:15:00", null, null, null, null, null, null, null, 1),
        rows("2024-07-01 00:16:00", null, null, 1, null, null, null, null, null),
        rows("2024-07-01 00:17:00", null, null, null, 1, null, null, null, null),
        rows("2024-07-01 00:18:00", 1, null, null, null, null, null, null, null),
        rows("2024-07-01 00:19:00", null, 1, null, null, null, null, null, null),
        rows("2024-07-01 00:20:00", null, null, null, null, 1, null, null, null));
    assertEquals(21, result.getInt("total"));
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
    assertThrowsWithReplace(
        ResponseException.class,
        () -> {
          executeQuery("source=no_timestamp | timechart count()");
        });
  }

  @Test
  public void testTimechartWithMinuteSpanNoGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m avg(cpu_usage)");
    verifySchema(result, schema("$f2", "timestamp"), schema("$f1", "double"));
    assertEquals(21, result.getInt("total"));
  }

  @Test
  public void testTimechartWithSecondSpanAndRegionGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1s count() by region");
    verifySchema(
        result,
        schema("$f2", "timestamp"),
        schema("eu-west", "bigint"),
        schema("us-east", "bigint"),
        schema("us-west", "bigint"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", null, 1, null),
        rows("2024-07-01 00:01:00", null, null, 1),
        rows("2024-07-01 00:02:00", null, 1, null),
        rows("2024-07-01 00:03:00", 1, null, null),
        rows("2024-07-01 00:04:00", null, null, 1),
        rows("2024-07-01 00:05:00", null, 1, null),
        rows("2024-07-01 00:06:00", 1, null, null),
        rows("2024-07-01 00:07:00", null, null, 1),
        rows("2024-07-01 00:08:00", null, 1, null),
        rows("2024-07-01 00:09:00", 1, null, null),
        rows("2024-07-01 00:10:00", null, null, 1),
        rows("2024-07-01 00:11:00", null, 1, null),
        rows("2024-07-01 00:12:00", 1, null, null),
        rows("2024-07-01 00:13:00", null, null, 1),
        rows("2024-07-01 00:14:00", null, 1, null),
        rows("2024-07-01 00:15:00", 1, null, null),
        rows("2024-07-01 00:16:00", null, 1, null),
        rows("2024-07-01 00:17:00", null, null, 1),
        rows("2024-07-01 00:18:00", null, 1, null),
        rows("2024-07-01 00:19:00", null, null, 1),
        rows("2024-07-01 00:20:00", null, 1, null));
    assertEquals(21, result.getInt("total"));
  }

  @Test
  public void testTimechartWithOtherCategory() throws IOException {
    // This test verifies that when there are more than 10 distinct values in the split-by field,
    // the top 10 values get their own columns and the rest are grouped into an "OTHER" column
    JSONObject result =
        executeQuery("source=events_many_hosts | timechart span=1h avg(cpu_usage) by host");

    // Verify schema has 12 columns: timestamp + 10 hosts + OTHER
    assertEquals(12, result.getJSONArray("schema").length());

    // First column should be timestamp
    assertEquals("$f3", result.getJSONArray("schema").getJSONObject(0).getString("name"));

    // Last column should be OTHER
    assertEquals("OTHER", result.getJSONArray("schema").getJSONObject(11).getString("name"));

    // Verify we have 1 data row (all events are at the same hour)
    assertEquals(1, result.getJSONArray("datarows").length());

    // Verify the OTHER column has a value (not null)
    Object otherValue = result.getJSONArray("datarows").getJSONArray(0).get(11);
    assertTrue("OTHER column should have a numeric value", otherValue instanceof Number);
  }

  @Test
  public void testTimechartWithLimit() throws IOException {
    JSONObject result =
        executeQuery("source=events | timechart span=1m limit=3 avg(cpu_usage) by host");

    // Verify schema has 5 columns: timestamp + 3 limited hosts in original order + OTHER
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-01", "double"),
        schema("web-02", "double"),
        schema("web-03", "double"),
        schema("OTHER", "double"));

    // Verify exact data rows match expected output
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", 45.2, null, null, null),
        rows("2024-07-01 00:01:00", null, 38.7, null, null),
        rows("2024-07-01 00:02:00", 55.3, null, null, null),
        rows("2024-07-01 00:03:00", null, null, 42.1, null),
        rows("2024-07-01 00:04:00", null, 41.8, null, null),
        rows("2024-07-01 00:05:00", 39.4, null, null, null),
        rows("2024-07-01 00:06:00", null, null, 48.6, null),
        rows("2024-07-01 00:07:00", null, 44.2, null, null),
        rows("2024-07-01 00:08:00", 67.8, null, null, null),
        rows("2024-07-01 00:09:00", null, null, 35.9, null),
        rows("2024-07-01 00:10:00", null, 43.1, null, null),
        rows("2024-07-01 00:11:00", 37.5, null, null, null),
        rows("2024-07-01 00:12:00", null, null, 59.7, null),
        rows("2024-07-01 00:13:00", null, 32.4, null, null),
        rows("2024-07-01 00:14:00", 49.8, null, null, null),
        rows("2024-07-01 00:15:00", null, null, 40.3, null),
        rows("2024-07-01 00:16:00", null, null, null, 78.2),
        rows("2024-07-01 00:17:00", null, null, null, 71.6),
        rows("2024-07-01 00:18:00", null, null, null, 15.8),
        rows("2024-07-01 00:19:00", null, null, null, 12.4),
        rows("2024-07-01 00:20:00", null, null, null, 8.9));

    assertEquals(21, result.getInt("total"));
  }

  @Test
  public void testTimechartWithLimitZero() throws IOException {
    // Test with limit=0 which means no limit (show all distinct values)
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=0 useother=t avg(cpu_usage) by"
                + " host");

    // Verify schema has 16 columns: timestamp + all 15 hosts (no OTHER column)
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-01", "double"),
        schema("web-02", "double"),
        schema("web-03", "double"),
        schema("web-04", "double"),
        schema("web-05", "double"),
        schema("web-06", "double"),
        schema("web-07", "double"),
        schema("web-08", "double"),
        schema("web-09", "double"),
        schema("web-10", "double"),
        schema("web-11", "double"),
        schema("web-12", "double"),
        schema("web-13", "double"),
        schema("web-14", "double"),
        schema("web-15", "double"));

    // Verify we have 1 data row with all hosts' values
    verifyDataRows(
        result,
        rows(
            "2024-07-01 00:00:00",
            45.2,
            38.7,
            55.3,
            42.1,
            41.8,
            39.4,
            48.6,
            44.2,
            67.8,
            35.9,
            43.1,
            37.5,
            59.7,
            32.4,
            49.8));

    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithUseOtherFalse() throws IOException {
    // Test with useother=false which should show top 10 hosts without an OTHER column
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h useother=false avg(cpu_usage) by host");

    // Verify schema has 11 columns: timestamp + top 10 hosts in original order (no OTHER column)
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-01", "double"), // Original order, but top 10 by score
        schema("web-03", "double"),
        schema("web-04", "double"),
        schema("web-05", "double"),
        schema("web-07", "double"),
        schema("web-08", "double"),
        schema("web-09", "double"),
        schema("web-11", "double"),
        schema("web-13", "double"),
        schema("web-15", "double"));

    // Verify we have 1 data row with top 10 hosts' values in original order
    verifyDataRows(
        result,
        rows(
            "2024-07-01 00:00:00",
            45.2, // web-01
            55.3, // web-03
            42.1, // web-04
            41.8, // web-05
            48.6, // web-07
            44.2, // web-08
            67.8, // web-09
            43.1, // web-11
            59.7, // web-13
            49.8)); // web-15

    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithLimitAndUseOther() throws IOException {
    // Test with limit=3 and useother=true (default) which should show top 3 hosts and an OTHER
    // column
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=3 useother=t avg(cpu_usage) by"
                + " host");

    // Verify schema has 5 columns: timestamp + 3 limited hosts in original order + OTHER
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-03", "double"), // Original order: web-03 (55.3)
        schema("web-09", "double"), // Original order: web-09 (67.8)
        schema("web-13", "double"), // Original order: web-13 (59.7)
        schema("OTHER", "double"));

    // Verify we have 1 data row with top 3 hosts' values in original order and OTHER
    verifyDataRows(result, rows("2024-07-01 00:00:00", 55.3, 67.8, 59.7, 498.7));

    assertEquals(1, result.getInt("total"));
  }
}
