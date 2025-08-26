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
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:04:00", "web-02", 1));
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
  public void testTimechartWithSecondSpanAndRegionGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1s count() by region");
    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("region", "string"),
        schema("count", "bigint"));
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "us-east", 1),
        rows("2024-07-01 00:01:00", "us-west", 1),
        rows("2024-07-01 00:02:00", "us-east", 1),
        rows("2024-07-01 00:03:00", "eu-west", 1),
        rows("2024-07-01 00:04:00", "us-west", 1));
    assertEquals(5, result.getInt("total"));
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
  public void testTimechartWithLimitZero() throws IOException {
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
  public void testTimechartWithUseOtherFalse() throws IOException {
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
  public void testTimechartWithCountAggregationShowsZeroInsteadOfNull() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("count", "bigint"));

    // In unpivoted format, we only have rows with actual values, not zeros
    verifyDataRows(
        result,
        rows("2024-07-01 00:00:00", "web-01", 1),
        rows("2024-07-01 00:01:00", "web-02", 1),
        rows("2024-07-01 00:02:00", "web-01", 1),
        rows("2024-07-01 00:03:00", "db-01", 1),
        rows("2024-07-01 00:04:00", "web-02", 1));

    assertEquals(5, result.getInt("total"));
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

    // Verify the top 3 hosts and OTHER category
    boolean foundWeb03 = false;
    boolean foundWeb07 = false;
    boolean foundWeb09 = false;
    boolean foundOther = false;
    double otherValue = 0.0;

    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      Object[] row = result.getJSONArray("datarows").getJSONArray(i).toList().toArray();
      String label = (String) row[1];

      if ("web-03".equals(label)) {
        foundWeb03 = true;
        assertEquals(55.3, ((Number) row[2]).doubleValue(), 0.01);
      } else if ("web-07".equals(label)) {
        foundWeb07 = true;
        assertEquals(48.6, ((Number) row[2]).doubleValue(), 0.01);
      } else if ("web-09".equals(label)) {
        foundWeb09 = true;
        assertEquals(67.8, ((Number) row[2]).doubleValue(), 0.01);
      } else if ("OTHER".equals(label)) {
        foundOther = true;
        otherValue = ((Number) row[2]).doubleValue();
      }
    }

    assertTrue("web-03 not found in results", foundWeb03);
    assertTrue("web-07 not found in results", foundWeb07);
    assertTrue("web-09 not found in results", foundWeb09);
    assertTrue("OTHER category not found in results", foundOther);

    // In the unpivoted format, OTHER is not a sum but represents each individual value
    // So we can't check for the exact sum value
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
}
