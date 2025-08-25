/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteBinCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.TIME_TEST_DATA);
  }

  @Test
  public void testBinWithNumericSpan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | fields age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    verifyDataRows(result, rows("20-30"), rows("20-30"), rows("20-30"));
  }

  @Test
  public void testBinNumericSpanPrecise() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 | fields balance | sort balance |" + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));

    verifyDataRows(result, rows("0-10000"), rows("0-10000"), rows("0-10000"));
  }

  @Test
  public void testBinWithBinsParameter() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin value bins=5 | fields value | sort value | head 3");
    verifySchema(result, schema("value", null, "string"));

    verifyDataRows(result, rows("6000-7000"), rows("6000-7000"), rows("6000-7000"));
  }

  @Test
  public void testBinWithMinspan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 | fields age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    verifyDataRows(result, rows("20-30"), rows("20-30"), rows("20-30"));
  }

  @Test
  public void testBinBasicFunctionality() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | bin age span=5 | fields age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    verifyDataRows(result, rows("30-35"), rows("35-40"), rows("25-30"));
  }

  @Test
  public void testBinLargeSpanValue() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=25000 | fields balance | sort balance |" + " head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));

    verifyDataRows(result, rows("0-25000"), rows("0-25000"));
  }

  @Test
  public void testBinValueFieldOnly() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin value span=2000"
                + " | fields value | head 3");
    verifySchema(result, schema("value", null, "string"));

    verifyDataRows(result, rows("8000-10000"), rows("6000-8000"), rows("8000-10000"));
  }

  @Test
  public void testBinWithStartEndBins() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 start=0 end=100 | fields age | sort age |" + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    // With bins=5 and start=0 end=100, expect equal-width bins based on actual data
    verifyDataRows(result, rows("20-30"), rows("20-30"), rows("20-30"));
  }

  @Test
  public void testBinWithStartEndBinsBalance() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=10 start=0 end=200000 | fields balance |"
                    + " sort balance | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));

    verifyDataRows(result, rows("0-10000"), rows("0-10000"), rows("0-10000"));
  }

  @Test
  public void testBinWithStartEndLargeRange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 start=0 end=1000 | fields age | sort age |" + " head 1",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    verifyDataRows(result, rows("20-30"));
  }

  @Test
  public void testBinWithTimestampSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=1h"
                + " | fields `@timestamp`, value | sort `@timestamp` | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));

    // With 1-hour spans
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", 8945),
        rows("2025-07-28 01:00:00", 7623),
        rows("2025-07-28 02:00:00", 9187));
  }

  @Test
  public void testBinWithTimestampStats() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=4h"
                + " | fields `@timestamp` | sort `@timestamp` | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"));

    // With 4-hour spans and stats
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"));
  }

  @Test
  public void testBinOnlyWithoutAggregation() throws IOException {
    // Test just the bin operation without aggregation
    JSONObject binOnlyResult =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=4h"
                + " | fields `@timestamp` | head 3");

    // Verify schema and that binning works correctly
    verifySchema(binOnlyResult, schema("@timestamp", null, "timestamp"));
    verifyDataRows(
        binOnlyResult,
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"));
  }

  @Test
  @Ignore
  // https://github.com/opensearch-project/sql/issues/4063
  public void testBinWithTimestampAggregation() throws IOException {
    // Test bin operation with fields only - no aggregation
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=4h"
                + " | fields `@timestamp` | sort `@timestamp` | head 3");

    // Verify schema
    verifySchema(result, schema("@timestamp", null, "timestamp"));

    // Verify that we get proper 4-hour time bins
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"),
        rows("2025-07-28 00:00:00"));
  }

  @Test
  public void testBinWithMonthlySpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=4mon as cate | fields"
                + " cate, @timestamp | head 5");
    verifySchema(result, schema("cate", null, "string"), schema("@timestamp", null, "timestamp"));

    // With 4-month spans using 'mon' unit
    verifyDataRows(
        result,
        rows("2025-05", "2025-07-28 00:15:23"),
        rows("2025-05", "2025-07-28 01:42:15"),
        rows("2025-05", "2025-07-28 02:28:45"),
        rows("2025-05", "2025-07-28 03:56:20"),
        rows("2025-05", "2025-07-28 04:33:10"));
  }

  @Test
  public void testBinAgeSpan5() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 | fields age | sort age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));
    verifyDataRows(result, rows("20-25"), rows("20-25"), rows("20-25"));
  }

  @Test
  public void testBinBalanceSpan1000() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1000 | fields balance | sort balance | head" + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("1000-2000"), rows("1000-2000"), rows("1000-2000"));
  }

  @Test
  public void testBinAgeBins2() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=2 | fields age | sort age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));
    verifyDataRows(result, rows("0-100"), rows("0-100"), rows("0-100"));
  }

  @Test
  public void testBinAgeBins21() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=21 | fields age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));
    verifyDataRows(result, rows("20-21"), rows("20-21"), rows("20-21"));
  }

  @Test
  public void testBinBalanceBins49() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=49 | fields balance | sort balance | head" + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("1000-2000"), rows("1000-2000"), rows("1000-2000"));
  }

  @Test
  public void testBinAgeMinspan101() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=101 | fields age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));
    verifyDataRows(result, rows("0-1000"), rows("0-1000"), rows("0-1000"));
  }

  @Test
  public void testBinAgeStartEndRange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=101 | fields age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));
    verifyDataRows(result, rows("0-100"), rows("0-100"), rows("0-100"));
  }

  @Test
  public void testBinBalanceStartEndRange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance start=0 end=100001 | fields balance | sort"
                    + " balance | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("0-100000"), rows("0-100000"), rows("0-100000"));
  }

  @Test
  public void testBinBalanceSpanLog10() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=log10 | fields balance | sort balance |" + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("1000.0-10000.0"), rows("1000.0-10000.0"), rows("1000.0-10000.0"));
  }

  @Test
  public void testBinBalanceSpan2Log10() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=2log10 | fields balance | sort balance |" + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("200.0-2000.0"), rows("200.0-2000.0"), rows("200.0-2000.0"));
  }

  @Test
  public void testBinBalanceSpanLog2() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=log2 | fields balance | sort balance | head" + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("1024.0-2048.0"), rows("1024.0-2048.0"), rows("1024.0-2048.0"));
  }

  @Test
  public void testBinBalanceSpan1Point5Log10() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1.5log10 | fields balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(result, rows("150.0-1500.0"), rows("150.0-1500.0"), rows("150.0-1500.0"));
  }

  @Test
  public void testBinBalanceSpanArbitraryLog() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1.11log2 | fields balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("balance", null, "string"));
    verifyDataRows(
        result, rows("1136.64-2273.28"), rows("1136.64-2273.28"), rows("1136.64-2273.28"));
  }

  @Test
  public void testBinTimestampSpan30Seconds() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=30seconds | fields"
                + " @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-28 00:15:00", 8945),
        rows("2025-07-28 01:42:00", 7623),
        rows("2025-07-28 02:28:30", 9187));
  }

  @Test
  public void testBinTimestampSpan45Minutes() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=45minute | fields"
                + " @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", 8945),
        rows("2025-07-28 01:30:00", 7623),
        rows("2025-07-28 02:15:00", 9187));
  }

  @Test
  public void testBinTimestampSpan7Days() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=7day | fields"
                + " @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-24 00:00:00", 8945),
        rows("2025-07-24 00:00:00", 7623),
        rows("2025-07-24 00:00:00", 9187));
  }

  @Test
  public void testBinTimestampSpan6Days() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=6day | fields"
                + " @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-23 00:00:00", 8945),
        rows("2025-07-23 00:00:00", 7623),
        rows("2025-07-23 00:00:00", 9187));
  }

  @Test
  public void testBinTimestampAligntimeHour() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=2h"
                + " aligntime='@d+3h' | fields @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-27 23:00:00", 8945),
        rows("2025-07-28 01:00:00", 7623),
        rows("2025-07-28 01:00:00", 9187));
  }

  @Test
  public void testBinTimestampAligntimeEpoch() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data | bin @timestamp span=2h"
                + " aligntime=1500000000 | fields @timestamp, value | sort @timestamp | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));
    verifyDataRows(
        result,
        rows("2025-07-27 22:40:00", 8945),
        rows("2025-07-28 00:40:00", 7623),
        rows("2025-07-28 00:40:00", 9187));
  }

  @Test
  public void testBinWithNonExistentField() {
    // Test that bin command throws an error when field doesn't exist in schema
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | bin non_existent_field span=10 | head 1", TEST_INDEX_ACCOUNT));
            });

    // Verify the error message contains information about the missing field
    String errorMessage = exception.getMessage();
    assertTrue(
        "Error message should mention the non-existent field: " + errorMessage,
        errorMessage.contains("non_existent_field") || errorMessage.contains("not found"));
  }

  @Test
  public void testBinSpanWithStartEndNeverShrinkRange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=1 start=25 end=35 as cate | fields cate, age | head 6",
                TEST_INDEX_BANK));

    verifySchema(result, schema("cate", null, "string"), schema("age", null, "int"));

    verifyDataRows(
        result,
        rows("32-33", 32),
        rows("36-37", 36),
        rows("28-29", 28),
        rows("33-34", 33),
        rows("36-37", 36),
        rows("39-40", 39));
  }

  @Test
  public void testBinFloatingPointSpanBasicFunctionality() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=2.5 | fields age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("age", null, "string"));

    // Test that floating point spans work with proper range formatting
    verifyDataRows(result, rows("27.5-30.0"), rows("30.0-32.5"), rows("35.0-37.5"));
  }

  @Test
  public void testBinFloatingPointSpanWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=15000.5 | fields balance | sort balance |"
                    + " head 2",
                TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("balance", null, "string"));

    // Test floating point spans without aggregation - verify proper decimal formatting
    verifyDataRows(result, rows("0.0-15000.5"), rows("0.0-15000.5"));
  }

  @Test
  @Ignore
  public void testBinWithNumericSpanStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  @Ignore
  public void testBinNumericSpanPreciseStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=10000 | stats count() by balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    verifyDataRows(
        result, rows(168L, "0-10000"), rows(213L, "10000-20000"), rows(217L, "20000-30000"));
  }

  @Test
  @Ignore
  public void testBinWithBinsParameterStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin value bins=5 | stats count() by value | sort value | head 3");
    verifySchema(result, schema("count()", null, "bigint"), schema("value", null, "string"));

    verifyDataRows(result, rows(24L, "6000-7000"), rows(25L, "7000-8000"), rows(33L, "8000-9000"));
  }

  @Test
  @Ignore
  public void testBinWithMinspanStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  @Ignore
  public void testBinLargeSpanValueStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=25000 | stats count() by balance | sort balance |"
                    + " head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    verifyDataRows(result, rows(485L, "0-25000"), rows(515L, "25000-50000"));
  }

  @Test
  @Ignore
  public void testBinWithStartEndBinsStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 start=0 end=100 | stats count() by age | sort age |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    // With bins=5 and start=0 end=100, expect equal-width bins based on actual data
    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  @Ignore
  public void testBinWithStartEndBinsBalanceStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=10 start=0 end=200000 | stats count() by balance |"
                    + " sort balance | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    verifyDataRows(
        result, rows(168L, "0-10000"), rows(213L, "10000-20000"), rows(217L, "20000-30000"));
  }

  @Test
  @Ignore
  public void testBinWithStartEndLargeRangeStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 start=0 end=1000 | stats count() by age | sort age |"
                    + " head 1",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"));
  }

  @Test
  @Ignore
  public void testBinWithTimestampAggregationStatsCount() throws IOException {
    // Test bin operation with aggregation - this should now work correctly
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=4h"
                + " | stats count() by `@timestamp` | sort `@timestamp` | head 3");

    // Verify schema
    verifySchema(
        result, schema("count()", null, "bigint"), schema("@timestamp", null, "timestamp"));

    // Verify that we get proper 4-hour time bins with expected counts
    // The time data spans across multiple 4-hour intervals
    verifyDataRows(
        result,
        rows(4L, "2025-07-28 00:00:00"),
        rows(4L, "2025-07-28 04:00:00"),
        rows(4L, "2025-07-28 08:00:00"));
  }

  @Test
  @Ignore
  public void testBinAgeSpan5StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));
    verifyDataRows(result, rows(225L, "20-25"), rows(226L, "25-30"), rows(259L, "30-35"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpan1000StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1000 | stats count() by balance | sort balance | head"
                    + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result, rows(19L, "1000-2000"), rows(26L, "10000-11000"), rows(24L, "11000-12000"));
  }

  @Test
  @Ignore
  public void testBinAgeBins2StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=2 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));
    verifyDataRows(result, rows(1000L, "0-100"));
  }

  @Test
  @Ignore
  public void testBinAgeBins21StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=21 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));
    verifyDataRows(result, rows(44L, "20-21"), rows(46L, "21-22"), rows(51L, "22-23"));
  }

  @Test
  @Ignore
  public void testBinBalanceBins49StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance bins=49 | stats count() by balance | sort balance | head"
                    + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result, rows(19L, "1000-2000"), rows(26L, "10000-11000"), rows(24L, "11000-12000"));
  }

  @Test
  @Ignore
  public void testBinAgeMinspan101StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=101 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));
    verifyDataRows(result, rows(1000L, "0-1000"));
  }

  @Test
  @Ignore
  public void testBinAgeStartEndRangeStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=101 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));
    verifyDataRows(result, rows(1000L, "0-100"));
  }

  @Test
  @Ignore
  public void testBinBalanceStartEndRangeStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance start=0 end=100001 | stats count() by balance | sort"
                    + " balance | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(result, rows(1000L, "0-100000"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpanLog10StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=log10 | stats count() by balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(result, rows(168L, "1000.0-10000.0"), rows(832L, "10000.0-100000.0"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpan2Log10StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=2log10 | stats count() by balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result,
        rows(19L, "200.0-2000.0"),
        rows(362L, "2000.0-20000.0"),
        rows(619L, "20000.0-200000.0"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpanLog2StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=log2 | stats count() by balance | sort balance | head"
                    + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result,
        rows(19L, "1024.0-2048.0"),
        rows(333L, "16384.0-32768.0"),
        rows(45L, "2048.0-4096.0"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpan1Point5Log10StatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1.5log10 | stats count() by balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result,
        rows(13L, "150.0-1500.0"),
        rows(266L, "1500.0-15000.0"),
        rows(721L, "15000.0-150000.0"));
  }

  @Test
  @Ignore
  public void testBinBalanceSpanArbitraryLogStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=1.11log2 | stats count() by balance | sort balance |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));
    verifyDataRows(
        result,
        rows(19L, "1136.64-2273.28"),
        rows(380L, "18186.24-36372.48"),
        rows(49L, "2273.28-4546.56"));
  }

  @Test
  @Ignore
  public void testBinFloatingPointSpanWithStatsCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin balance span=15000.5 | stats count() by balance | sort balance |"
                    + " head 2",
                TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    // Test floating point spans with stats aggregation - verify proper decimal formatting
    verifyDataRows(result, rows(279L, "0.0-15000.5"), rows(319L, "15000.5-30001.0"));
  }
}
