/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
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
    String eventsMapping = "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"},\"service\":{\"type\":\"keyword\"},\"response_time\":{\"type\":\"integer\"},\"status_code\":{\"type\":\"integer\"},\"bytes_sent\":{\"type\":\"long\"},\"cpu_usage\":{\"type\":\"double\"},\"memory_usage\":{\"type\":\"double\"},\"region\":{\"type\":\"keyword\"},\"environment\":{\"type\":\"keyword\"}}}}";
    if (!isIndexExist(client(), "events")) {
      createIndexByRestClient(client(), "events", eventsMapping);
      loadDataByRestClient(client(), "events", "src/test/resources/events_test.json");
    }
  }

  @Test
  public void testTimechartWithHourSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1h count() by host");
    verifySchema(result, schema("$f2", "timestamp"), schema("cache-01", "bigint"), schema("cache-02", "bigint"), schema("db-01", "bigint"), schema("db-02", "bigint"), schema("lb-01", "bigint"), schema("web-01", "bigint"), schema("web-02", "bigint"), schema("web-03", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:00:00", 1, 1, 1, 1, 1, 6, 5, 5));
    assertEquals(1, result.getInt("total"));
  }

  @Test
  public void testTimechartWithMinuteSpanAndGroupBy() throws IOException {
    JSONObject result = executeQuery("source=events | timechart span=1m count() by host");
    verifySchema(result, schema("$f2", "timestamp"), schema("cache-01", "bigint"), schema("cache-02", "bigint"), schema("db-01", "bigint"), schema("db-02", "bigint"), schema("lb-01", "bigint"), schema("web-01", "bigint"), schema("web-02", "bigint"), schema("web-03", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:00:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:01:00", null, null, null, null, null, null, 1, null), rows("2024-07-01 00:02:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:03:00", null, null, null, null, null, null, null, 1), rows("2024-07-01 00:04:00", null, null, null, null, null, null, 1, null), rows("2024-07-01 00:05:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:06:00", null, null, null, null, null, null, null, 1), rows("2024-07-01 00:07:00", null, null, null, null, null, null, 1, null), rows("2024-07-01 00:08:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:09:00", null, null, null, null, null, null, null, 1), rows("2024-07-01 00:10:00", null, null, null, null, null, null, 1, null), rows("2024-07-01 00:11:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:12:00", null, null, null, null, null, null, null, 1), rows("2024-07-01 00:13:00", null, null, null, null, null, null, 1, null), rows("2024-07-01 00:14:00", null, null, null, null, null, 1, null, null), rows("2024-07-01 00:15:00", null, null, null, null, null, null, null, 1), rows("2024-07-01 00:16:00", null, null, 1, null, null, null, null, null), rows("2024-07-01 00:17:00", null, null, null, 1, null, null, null, null), rows("2024-07-01 00:18:00", 1, null, null, null, null, null, null, null), rows("2024-07-01 00:19:00", null, 1, null, null, null, null, null, null), rows("2024-07-01 00:20:00", null, null, null, null, 1, null, null, null));
    assertEquals(21, result.getInt("total"));
  }

  @Test
  public void testTimechartWithoutTimestampField() throws IOException {
    // Create index without @timestamp field
    String noTimestampMapping = "{\"mappings\":{\"properties\":{\"name\":{\"type\":\"keyword\"},\"occupation\":{\"type\":\"keyword\"},\"country\":{\"type\":\"keyword\"},\"salary\":{\"type\":\"integer\"},\"year\":{\"type\":\"integer\"},\"month\":{\"type\":\"integer\"}}}}";
    if (!isIndexExist(client(), "no_timestamp")) {
      createIndexByRestClient(client(), "no_timestamp", noTimestampMapping);
      loadDataByRestClient(client(), "no_timestamp", "src/test/resources/occupation.json");
    }
    
    // Test should throw exception for missing @timestamp field
    assertThrowsWithReplace(ResponseException.class, () -> {
      executeQuery("source=no_timestamp | timechart count()");
    });
  }
}