/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.*;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
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
    loadIndex(Index.BANK);
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
  }

  @Test
  public void testTimechartWithMinuteSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

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
    Throwable exception =
        assertThrows(
            ResponseException.class,
            () -> {
              executeQuery(String.format("source=%s | timechart count()", TEST_INDEX_BANK));
            });
    verifyErrorMessageContains(exception, "Field [@timestamp] not found.");
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
    verifyNumOfRows(result, 11);
    verifyDataRowsSome(result, rows("2024-07-01 00:00:00", "OTHER", 35.9));
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
    verifyNumOfRows(result, 11);

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

    verifyNumOfRows(result, 11);
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
    verifyNumOfRows(result, 10);

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

    verifyNumOfRows(result, 10);
  }

  @Test
  public void testTimechartWithCountNoLimitByHostShowZero() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count()", "bigint"));

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

    verifyDataRows(
        result,
        closeTo("2024-07-01 00:00:00", "OTHER", 41.300000000000004),
        closeTo("2024-07-01 00:00:00", "web-03", 55.3),
        closeTo("2024-07-01 00:00:00", "web-07", 48.6),
        closeTo("2024-07-01 00:00:00", "web-09", 67.8));
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
