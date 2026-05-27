/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assume.assumeTrue;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Regression net for sql#5420 on the analytics-engine route. Pins datetime wire format ({@code
 * yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]}, typed schema labels) and asserts every query was served by AE —
 * without the routing pin, a silent fallback to Calcite would leave the assertions green (Calcite
 * already emits the documented format). Skipped on the legacy path.
 */
public class CalciteAnalyticsDatetimeWireFormatIT extends PPLIntegTestCase {

  private static final String INDEX = "wire_format_dt";

  @Override
  public void init() throws Exception {
    super.init();
    assumeTrue(
        "CalciteAnalyticsDatetimeWireFormatIT only meaningful with"
            + " -Dtests.analytics.parquet_indices=true",
        isAnalyticsParquetIndicesEnabled());
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), INDEX)) {
      String mapping =
          "{\"mappings\":{\"properties\":{"
              + "\"ts\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd HH:mm:ss\"},"
              + "\"ts_nanos\":{\"type\":\"date_nanos\"},"
              + "\"d\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd\"},"
              + "\"t\":{\"type\":\"date\",\"format\":\"HH:mm:ss\"}}}}";
      TestUtils.createIndexByRestClient(client(), INDEX, mapping);

      Request doc = new Request("PUT", "/" + INDEX + "/_doc/1?refresh=true");
      doc.setJsonEntity(
          "{\"ts\":\"2024-03-15 10:30:00\","
              + "\"ts_nanos\":\"2024-03-15T10:30:00.123456789Z\","
              + "\"d\":\"2024-03-15\","
              + "\"t\":\"10:30:00\"}");
      client().performRequest(doc);

      Request doc2 = new Request("PUT", "/" + INDEX + "/_doc/2?refresh=true");
      doc2.setJsonEntity(
          "{\"ts\":\"2024-03-16 23:59:59\","
              + "\"ts_nanos\":\"2024-03-16T23:59:59.999999999Z\","
              + "\"d\":\"2024-03-16\","
              + "\"t\":\"23:59:59\"}");
      client().performRequest(doc2);
    }
  }

  /**
   * AE route: {@code LogicalTableScan} + lowercase {@code opensearch}. Calcite legacy uses {@code
   * CalciteLogicalIndexScan}.
   */
  private void assertRoutedToAnalyticsEngine(String query) throws IOException {
    String explained = explainQueryToString(query);
    Assert.assertTrue(
        "Expected analytics-engine route (LogicalTableScan + lowercase 'opensearch'), got: "
            + explained,
        explained.contains("LogicalTableScan(table=[[opensearch,"));
    Assert.assertFalse(
        "Expected analytics-engine route, but query routed to Calcite legacy"
            + " (CalciteLogicalIndexScan): "
            + explained,
        explained.contains("CalciteLogicalIndexScan"));
  }

  /** TIMESTAMP root col: typed schema + space-separator value. */
  @Test
  public void testTimestampRootColumnSpaceFormat() throws IOException {
    String query = "source=" + INDEX + " | where ts = '2024-03-15 10:30:00' | fields ts";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("ts", "timestamp"));
    verifyDataRows(result, rows("2024-03-15 10:30:00"));
  }

  /**
   * DATE-mapped col: AE widens to TIMESTAMP at scan time; value must use space separator, not ISO
   * {@code T}.
   */
  @Test
  public void testDateRootColumnYmdFormat() throws IOException {
    String query = "source=" + INDEX + " | where d = '2024-03-15' | fields d";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("d", "timestamp"));
    verifyDataRows(result, rows("2024-03-15 00:00:00"));
  }

  /** TIME-mapped col: AE widens to TIMESTAMP; value must use space separator, not ISO {@code T}. */
  @Test
  public void testTimeRootColumnHmsFormat() throws IOException {
    String query = "source=" + INDEX + " | sort t | head 1 | fields t";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("t", "timestamp"));
    Assert.assertFalse(
        "Time-mapped column must not surface as ISO T-separator literal",
        result.getJSONArray("datarows").getJSONArray(0).getString(0).contains("T"));
  }

  /** Eval-derived TIMESTAMP follows the same wire-format contract as a root column. */
  @Test
  public void testEvalDerivedTimestampSpaceFormat() throws IOException {
    String query =
        "source=" + INDEX + " | where ts = '2024-03-15 10:30:00' | eval x = ts | fields x";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("x", "timestamp"));
    verifyDataRows(result, rows("2024-03-15 10:30:00"));
  }

  /** {@code min(ts)} returns a typed timestamp cell, not a stringified ISO-T literal. */
  @Test
  public void testStatsMinTimestampSpaceFormat() throws IOException {
    String query = "source=" + INDEX + " | stats min(ts) as min_ts";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("min_ts", "timestamp"));
    verifyDataRows(result, rows("2024-03-15 10:30:00"));
  }

  /**
   * AE parses indexed TIMESTAMP as a real timestamp for WHERE comparison (not lex string compare).
   */
  @Test
  public void testTimestampWhereComparisonFiltersCorrectly() throws IOException {
    String matchQuery = "source=" + INDEX + " | where ts > '2024-03-16 00:00:00' | fields ts";
    assertRoutedToAnalyticsEngine(matchQuery);
    JSONObject match = executeQuery(matchQuery);
    verifySchema(match, schema("ts", "timestamp"));
    verifyDataRows(match, rows("2024-03-16 23:59:59"));

    JSONObject miss =
        executeQuery("source=" + INDEX + " | where ts < '2024-03-15 00:00:00' | fields ts");
    Assert.assertEquals(
        "Strict comparison should exclude both rows when bound is before any seeded timestamp",
        0,
        miss.getJSONArray("datarows").length());
  }

  /**
   * {@code year/month/day_of_month/hour} extract calendar fields from the parsed TIMESTAMP, not a
   * stringified form.
   */
  @Test
  public void testTimestampScalarExtractFunctions() throws IOException {
    String query =
        "source="
            + INDEX
            + " | where ts = '2024-03-15 10:30:00'"
            + " | eval y = year(ts), m = month(ts), dm = day_of_month(ts), h = hour(ts) "
            + "| fields y, m, dm, h";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(
        result, schema("y", "int"), schema("m", "int"), schema("dm", "int"), schema("h", "int"));
    verifyDataRows(result, rows(2024, 3, 15, 10));
  }

  /**
   * ORDER BY on TIMESTAMP returns rows ascending; schema stays {@code timestamp}, values use space
   * separator.
   */
  @Test
  public void testTimestampOrderByTemporalSemantics() throws IOException {
    String query = "source=" + INDEX + " | sort ts | fields ts";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("ts", "timestamp"));
    verifyDataRows(result, rows("2024-03-15 10:30:00"), rows("2024-03-16 23:59:59"));
  }

  /**
   * {@code date_nanos} preserves 9-digit sub-second precision end-to-end (catches micro-truncation
   * regressions).
   */
  @Test
  public void testTimestampNanoPrecisionTrailingNines() throws IOException {
    String query = "source=" + INDEX + " | sort ts_nanos | fields ts_nanos";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("ts_nanos", "timestamp"));
    verifyDataRows(
        result, rows("2024-03-15 10:30:00.123456789"), rows("2024-03-16 23:59:59.999999999"));
  }

  /** {@code max(ts)} returns a typed timestamp cell with the documented wire format. */
  @Test
  public void testStatsMaxTimestampSpaceFormat() throws IOException {
    String query = "source=" + INDEX + " | stats max(ts) as max_ts";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifySchema(result, schema("max_ts", "timestamp"));
    verifyDataRows(result, rows("2024-03-16 23:59:59"));
  }

  /**
   * {@code dc(ts)} on two distinct timestamps returns 2 (AE dedups by temporal identity, not string
   * equality).
   */
  @Test
  public void testStatsCountDistinctTimestamp() throws IOException {
    String query = "source=" + INDEX + " | stats dc(ts) as n";
    assertRoutedToAnalyticsEngine(query);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(2));
  }
}
