/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTimechartPerFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    loadIndex(Index.EVENTS_TRAFFIC);
  }

  @Test
  public void testTimechartPerSecondWithDefaultSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_traffic | where month(@timestamp) = 9 | timechart per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", 1.0), // 60 / 1m
        rows("2025-09-08 10:01:00", 2.0), // 120 / 1m
        rows("2025-09-08 10:02:00", 4.0)); // (60+180) / 1m
  }

  @Test
  public void testTimechartPerSecondWithSpecifiedSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_traffic | where month(@timestamp) = 9 | timechart span=2m"
                + " per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", 1.5), // (60+120) / 2m
        rows("2025-09-08 10:02:00", 2.0)); // (60+180) / 2m
  }

  @Test
  public void testTimechartPerSecondWithByClause() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_traffic | where month(@timestamp) = 9 | timechart span=2m"
                + " per_second(packets) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(packets)", "double"));
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", "server1", 1.5), // (60+120) / 2m
        rows("2025-09-08 10:02:00", "server1", 0.5), // 60 / 2m
        rows("2025-09-08 10:02:00", "server2", 1.5)); // 180 / 2m
  }

  @Test
  public void testTimechartPerSecondWithLimitAndByClause() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_traffic | where month(@timestamp) = 9 | timechart span=2m limit=1"
                + " per_second(packets) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(packets)", "double"));
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", "server1", 1.5),
        rows("2025-09-08 10:02:00", "server1", 0.5),
        rows("2025-09-08 10:02:00", "OTHER", 1.5));
  }

  @Test
  public void testTimechartPerSecondWithVariableMonthLengths() throws IOException {
    JSONObject result =
        executeQuery(
            "source=events_traffic | where month(@timestamp) != 9 | timechart span=1M"
                + " per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));
    verifyDataRows(
        result,
        rows("2025-02-01 00:00:00", 7.75), // 18748800 / 28 days' seconds
        rows("2025-10-01 00:00:00", 7.0)); // 18748800 / 31 days' seconds
  }
}
