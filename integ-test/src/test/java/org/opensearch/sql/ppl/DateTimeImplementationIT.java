/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class DateTimeImplementationIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DATE);
    loadIndex(Index.DATE_FORMATS);
  }

  @Test
  public void inRangeZeroToStringTZ() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-24 21:30:00"));
  }

  @Test
  public void inRangeZeroToPositive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+01:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 06:30:00"));
  }

  @Test
  public void inRangeNegativeToPositive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00-05:00', '+05:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 15:30:00"));
  }

  @Test
  public void inRangeTwentyHourOffset() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2004-02-28 23:00:00-10:00', '+10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2004-02-29 19:00:00"));
  }

  @Test
  public void inRangeYearChange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00', '-10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2007-12-31 06:00:00"));
  }

  @Test
  public void inRangeZeroToMax() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+14:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 19:30:00"));
  }

  @Test
  public void inRangeNoToTZ() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));
  }

  @Test
  public void inRangeNoTZ() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));
  }

  @Test
  public void nullField3Over() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+15:00', '-12:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullField2Under() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00', '-14:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullTField3Over() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00', '+15:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueFebruary() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2021-02-30 10:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueApril() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2021-04-31 10:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueMonth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2021-13-03 10:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void testSpanDatetimeWithCustomFormat() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval a = 1 | stats count() as cnt by span(yyyy-MM-dd, 1d) as span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(result, schema("cnt", null, "integer"), schema("span", null, "date"));
    verifyDataRows(result, rows(2, "1984-04-12"));
  }
}
