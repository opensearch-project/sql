/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.DateTimeFunctionIT;

public class CalciteDateTimeFunctionIT extends DateTimeFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  // TODO: Remove this when supporting type coercion and casting with Calcite
  //  https://github.com/opensearch-project/sql/issues/3761
  @Ignore
  @Override
  public void testUnixTimestampWithTimestampString() throws IOException {}

  @Test
  public void testStrftimeWithComplexFormat() throws IOException {
    // Test strftime with complex format string including various date/time components
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%a, %b %d, %Y %I:%M:%S %p %Z"));
    verifyDataRows(result, rows("Mon, Mar 19, 2018 01:55:03 PM UTC"));
  }

  @Test
  public void testStrftimeWithVariousInputTypes() throws IOException {
    // Test 1: Direct UNIX timestamp (INTEGER/LONG)
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d %H:%M:%S"));
    verifyDataRows(result1, rows("2018-03-19 13:55:03"));

    // Test 2: Direct use with now() - TIMESTAMP type
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(now(), '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    // Verify the result is today's date in YYYY-MM-DD format
    String todayDate = LocalDate.now(ZoneOffset.UTC).toString();
    verifyDataRows(result2, rows(todayDate));

    // Test 3: Double timestamp with milliseconds
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703.123456, '%s') | fields result | head"
                    + " 1",
                TEST_INDEX_DATE, "%Y-%m-%d %H:%M:%S.%3Q"));
    verifyDataRows(result3, rows("2018-03-19 13:55:03.123"));
  }

  @Test
  public void testStrftimeWithDateFields() throws IOException {
    // Test strftime with different date field types from indices
    loadIndex(Index.DATE_FORMATS);

    // Test 1: Direct use with date field (TIMESTAMP type from field)
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | eval formatted = strftime(epoch_millis, '%s') | "
                    + "fields epoch_millis, formatted | head 1",
                TEST_INDEX_DATE_FORMATS, "%Y-%m-%d %H:%M:%S"));
    verifyDataRows(result1, rows("1984-04-12 09:07:42.000123456", "1984-04-12 09:07:42"));

    // Test 2: Using with unix_timestamp conversion
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | eval unix_ts = unix_timestamp(date_time) | "
                    + "eval formatted = strftime(unix_ts, '%s') | "
                    + "fields formatted | head 1",
                TEST_INDEX_DATE_FORMATS, "%F"));
    verifyDataRows(result2, rows("1984-04-12"));
  }

  @Test
  public void testStrftimeWithExpressions() throws IOException {
    // Test strftime with various expressions as input

    // Test 1: Mathematical expression
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | eval ts = 1521467703 + 86400 | "
                    + "eval result = strftime(ts, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result1, rows("2018-03-20")); // One day after

    // Test 2: Conditional expression with now()
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(now(), '%s') | " + "fields result | head 1",
                TEST_INDEX_DATE, "%Y"));
    String currentYear = String.valueOf(LocalDate.now(ZoneOffset.UTC).getYear());
    verifyDataRows(result2, rows(currentYear));
    // Should return current year

    // Test 3: Using unix_timestamp to convert string first
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s | eval ts = unix_timestamp('2018-03-19 13:55:03') | "
                    + "eval result = strftime(ts, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%m/%d/%Y"));
    verifyDataRows(result3, rows("03/19/2018"));
  }

  @Test
  public void testStrftimeStringHandling() throws IOException {
    // Test 1: Support string literal
    JSONObject result0 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime('1521467703', '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result0, rows("2018-03-19"));

    // Test 2: The correct approach - use numeric literals directly
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result1, rows("2018-03-19"));

    // Test 3: For date strings, users must use unix_timestamp() first
    // This is the recommended approach for converting date strings
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | eval ts = unix_timestamp('2018-03-19 13:55:03') | "
                    + "eval result = strftime(ts, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result2, rows("2018-03-19"));
  }

  @Test
  public void testStrftimeWithNegativeTimestamps() throws IOException {
    // Test strftime with negative timestamps (dates before 1970)

    // Test 1: -1 represents 1969-12-31 23:59:59 UTC
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(-1, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d %H:%M:%S"));
    verifyDataRows(result1, rows("1969-12-31 23:59:59"));

    // Test 2: -86400 represents 1969-12-31 00:00:00 UTC (one day before epoch)
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(-86400, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result2, rows("1969-12-31"));

    // Test 3: -31536000 represents 1969-01-01 00:00:00 UTC (one year before epoch)
    JSONObject result3 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(-31536000, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result3, rows("1969-01-01"));

    // Test 4: Large negative timestamp for older dates
    // -946771200 represents 1940-01-01 00:00:00 UTC
    JSONObject result4 =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(-946771200, '%s') | fields result | head 1",
                TEST_INDEX_DATE, "%Y-%m-%d"));
    verifyDataRows(result4, rows("1940-01-01"));
  }
}
