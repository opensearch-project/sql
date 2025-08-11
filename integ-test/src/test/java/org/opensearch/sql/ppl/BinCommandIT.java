/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class BinCommandIT extends PPLIntegTestCase {

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
                "source=%s | bin age span=10 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testBinNumericSpanPrecise() throws IOException {
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
  public void testBinWithBinsParameter() throws IOException {
    JSONObject result =
        executeQuery(
            "source=opensearch-sql_test_index_time_data"
                + " | bin value bins=5 | stats count() by value | sort value | head 3");
    verifySchema(result, schema("count()", null, "bigint"), schema("value", null, "string"));

    verifyDataRows(result, rows(24L, "6000-7000"), rows(25L, "7000-8000"), rows(33L, "8000-9000"));
  }

  @Test
  public void testBinWithMinspan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
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
                "source=%s | bin balance span=25000 | stats count() by balance | sort balance |"
                    + " head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("balance", null, "string"));

    verifyDataRows(result, rows(485L, "0-25000"), rows(515L, "25000-50000"));
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
                "source=%s | bin age bins=5 start=0 end=100 | stats count() by age | sort age |"
                    + " head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    // With bins=5 and start=0 end=100, expect equal-width bins based on actual data
    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testBinWithStartEndBinsBalance() throws IOException {
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
  public void testBinWithStartEndLargeRange() throws IOException {
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
}
