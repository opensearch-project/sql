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

    loadIndex(Index.TIMECHART_PER_FUNCTION);
  }

  @Test
  public void testTimechartPerSecondWithDefaultSpan() throws IOException {
    JSONObject result =
        executeQuery("source=timechart_per_function_test | timechart span=1m per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    // With 1m span: 60/60=1.0, 120/60=2.0, (180+30)/60=3.5
    // The 10:02:30 data falls into the 10:02:00 bucket
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", 1.0),
        rows("2025-09-08 10:01:00", 2.0),
        rows("2025-09-08 10:02:00", 3.5));
  }

  @Test
  public void testTimechartPerSecondWithTwentySecondSpan() throws IOException {
    JSONObject result =
        executeQuery("source=timechart_per_function_test | timechart span=20s per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    // With 20s span: 60/20=3.0, 120/20=6.0, 180/20=9.0, 30/20=1.5 (correct floating point)
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", 3.0),
        rows("2025-09-08 10:01:00", 6.0),
        rows("2025-09-08 10:02:00", 9.0),
        rows("2025-09-08 10:02:20", 1.5));
  }

  @Test
  public void testTimechartPerSecondWithOneHourSpan() throws IOException {
    JSONObject result =
        executeQuery("source=timechart_per_function_test | timechart span=1h per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    // With 1h span: (60+120+180+30)/3600 = 390/3600 = 0.10833333333333334
    verifyDataRows(result, rows("2025-09-08 10:00:00", 0.10833333333333334));
  }

  @Test
  public void testTimechartPerSecondWithByLimitUseother() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timechart_per_function_test | timechart span=1m limit=1 useother=true"
                + " per_second(packets) by host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(packets)", "double"));

    // With limit=1 and useother=true, expected 4 rows instead of 6
    // server1 appears in buckets, OTHER shows correct floating point: 30/60 = 0.5
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", "server1", 1.0),
        rows("2025-09-08 10:01:00", "server1", 2.0),
        rows("2025-09-08 10:02:00", "server1", 3.0),
        rows("2025-09-08 10:02:00", "OTHER", 0.5)); // OTHER shows correct 30/60 = 0.5
  }

  @Test
  public void testTimechartPerSecondDataRows() throws IOException {
    JSONObject result =
        executeQuery("source=timechart_per_function_test | timechart span=1m per_second(packets)");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("per_second(packets)", "double"));

    // Verify the same data as testTimechartPerSecondWithDefaultSpan
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", 1.0),
        rows("2025-09-08 10:01:00", 2.0),
        rows("2025-09-08 10:02:00", 3.5));
  }

  @Test
  public void testTimechartPerSecondWithByClauseAndLimit() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timechart_per_function_test | timechart span=30s limit=2 per_second(packets) by"
                + " host");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("host", "string"),
        schema("per_second(packets)", "double"));

    // With 30s span and limit=2, expected 4 rows instead of 8
    // Only buckets with actual data, no zero-filled buckets
    verifyDataRows(
        result,
        rows("2025-09-08 10:00:00", "server1", 2.0), // 60/30 = 2.0
        rows("2025-09-08 10:01:00", "server1", 4.0), // 120/30 = 4.0
        rows("2025-09-08 10:02:00", "server1", 6.0), // 180/30 = 6.0
        rows("2025-09-08 10:02:30", "server2", 1.0)); // 30/30 = 1.0
  }
}
