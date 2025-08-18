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
        schema("db-01", "bigint"),
        schema("web-01", "bigint"),
        schema("web-02", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:00:00", 1, 2, 2));
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithMinuteSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");
    verifySchema(
        result,
        schema("$f2", "timestamp"),
        schema("db-01", "bigint"),
        schema("web-01", "bigint"),
        schema("web-02", "bigint"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", null, 1, null),
        rows("2024-07-01 00:01:00", null, null, 1),
        rows("2024-07-01 00:02:00", null, 1, null),
        rows("2024-07-01 00:03:00", 1, null, null),
        rows("2024-07-01 00:04:00", null, null, 1));
    assertEquals(5, result.getInt("total"));
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
    assertEquals(5, result.getInt("total"));
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
        rows("2024-07-01 00:04:00", null, null, 1));
    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testTimechartWithOtherCategory() throws IOException {
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

    // Verify the OTHER column has a value (not null) - should contain web-10 (lowest value)
    Object otherValue = result.getJSONArray("datarows").getJSONArray(0).get(11);
    assertEquals(35.9, ((Number) otherValue).doubleValue(), 0.01);
  }

  @Test
  public void testTimechartWithLimit() throws IOException {
    JSONObject result =
        executeQuery("source=events | timechart span=1m limit=2 avg(cpu_usage) by host");

    // Verify schema has 4 columns: timestamp + 2 limited hosts + OTHER
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-01", "double"),
        schema("web-02", "double"),
        schema("OTHER", "double"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", 45.2, null, null),
        rows("2024-07-01 00:01:00", null, 38.7, null),
        rows("2024-07-01 00:02:00", 55.3, null, null),
        rows("2024-07-01 00:03:00", null, null, 42.1),
        rows("2024-07-01 00:04:00", null, 41.8, null));

    assertEquals(5, result.getInt("total"));
  }

  @Test
  public void testTimechartWithLimitZero() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=0 useother=t avg(cpu_usage) by"
                + " host");

    // Verify schema has 12 columns: timestamp + all 11 hosts (no OTHER column)
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
        schema("web-11", "double"));

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
            43.1));

    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithUseOtherFalse() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h useother=false avg(cpu_usage) by host");

    // Verify schema has 11 columns: timestamp + top 10 hosts (no OTHER column)
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
        schema("web-11", "double"));

    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", 45.2, 38.7, 55.3, 42.1, 41.8, 39.4, 48.6, 44.2, 67.8, 43.1));

    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithLimitAndUseOther() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_many_hosts | timechart span=1h limit=3 useother=t avg(cpu_usage) by"
                + " host");

    // Verify schema has 5 columns: timestamp + 3 limited hosts + OTHER
    verifySchema(
        result,
        schema("$f3", "timestamp"),
        schema("web-03", "double"),
        schema("web-07", "double"),
        schema("web-09", "double"),
        schema("OTHER", "double"));

    // OTHER = sum of remaining 8 hosts = 330.4
    verifyDataRows(result, rows("2024-07-01 00:00:00", 55.3, 48.6, 67.8, 330.4));

    assertEquals(1, result.getInt("total"));
  }
}
