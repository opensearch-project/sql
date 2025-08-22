/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class StrftimeFunctionIT extends PPLIntegTestCase {

  private static Locale originalLocale;

  @BeforeAll
  public static void setUpLocale() {
    // Save the original locale
    originalLocale = Locale.getDefault();
    // Set locale to US for consistent test results
    Locale.setDefault(Locale.US);
  }

  @AfterAll
  public static void restoreLocale() {
    // Restore the original locale after tests
    Locale.setDefault(originalLocale);
  }

  @Override
  public void init() throws IOException {
    loadIndex(Index.PEOPLE);
    enableCalcite();
  }

  @Test
  public void testStrftimeWithUnixTimestamp() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%Y-%m-%dT%H:%M:%S"));
    verifySchema(result, schema("result", null, "string"));
    verifyDataRows(result, rows("2018-03-19T13:55:03"));
  }

  @Test
  public void testStrftimeWithLongTimestamp() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703049000000, '%s') | fields result |"
                    + " head 1",
                TEST_INDEX_PEOPLE, "%Y-%m-%dT%H:%M:%S.%Q"));
    verifyDataRows(result, rows("2018-03-19T13:55:03.000"));
  }

  @Test
  public void testStrftimeWithISOFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%F %T"));
    verifyDataRows(result, rows("2018-03-19 13:55:03"));
  }

  @Test
  public void testStrftimeWithWeekdayMonthFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%a %b %d, %Y"));
    verifyDataRows(result, rows("Mon Mar 19, 2018"));
  }

  @Test
  public void testStrftimeWith12HourFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%I:%M:%S %p"));
    verifyDataRows(result, rows("01:55:03 PM"));
  }

  @Test
  public void testStrftimeWithSubseconds() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703123456789, '%s') | fields result |"
                    + " head 1",
                TEST_INDEX_PEOPLE, "%S.%3Q"));
    verifyDataRows(result, rows("03.000"));
  }

  @Test
  public void testStrftimeWithMicroseconds() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%f"));
    verifyDataRows(result, rows("000000"));
  }

  @Test
  public void testStrftimeWithEpochSeconds() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%s"));
    verifyDataRows(result, rows("1521467703"));
  }

  @Test
  public void testStrftimeWithWeekNumbers() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%V %U %w"));
    verifyDataRows(result, rows("12 11 1"));
  }

  @Test
  public void testStrftimeWithDayOfYear() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%j"));
    verifyDataRows(result, rows("078"));
  }

  @Test
  public void testStrftimeWithCentury() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%C%y"));
    verifyDataRows(result, rows("2018"));
  }

  @Test
  public void testStrftimeWithPercentLiteral() throws IOException {
    // Testing literal percent: %% should produce %
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%%Y"));
    verifyDataRows(result, rows("%Y"));
  }

  @Test
  public void testStrftimeWithComplexFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, 'Date: %s, Time: %s') | fields"
                    + " result | head 1",
                TEST_INDEX_PEOPLE, "%F", "%T"));
    verifyDataRows(result, rows("Date: 2018-03-19, Time: 13:55:03"));
  }

  @Disabled(
      "PPL doesn't support null literal directly - covered by testStrftimeWithInvalidTimestamp")
  @Test
  public void testStrftimeWithNullTimestamp() throws IOException {
    // Since PPL doesn't support null literal directly and invalid timestamps return null,
    // we can verify this behavior with the invalid timestamp test
    // This test is covered by testStrftimeWithInvalidTimestamp
    // which tests that invalid/negative timestamps return null
  }

  @Test
  public void testStrftimeWithInvalidTimestamp() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(-1, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%Y-%m-%d"));
    verifyDataRows(result, rows((Object) null));
  }

  @Test
  public void testStrftimeWithStringTimestamp() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime('1521467703', '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%Y-%m-%d"));
    verifyDataRows(result, rows("2018-03-19"));
  }

  @Test
  public void testStrftimeWithMultipleFormats() throws IOException {
    // Test combining multiple format specifiers
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%A %B %d %Y at %I:%M %p"));
    verifyDataRows(result, rows("Monday March 19 2018 at 01:55 PM"));
  }

  @Test
  public void testStrftimeWithTimezoneFormats() throws IOException {
    // Since we use UTC, timezone should be UTC/Z
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1521467703, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%Z %z"));
    verifyDataRows(result, rows("UTC +0000"));
  }

  @Test
  public void testStrftimeWithSpacePaddedFormats() throws IOException {
    // Test space-padded formats %e and %k
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval result = strftime(1517472303, '%s') | fields result | head 1",
                TEST_INDEX_PEOPLE, "%e %k"));
    verifyDataRows(result, rows(" 1  8"));
  }
}
