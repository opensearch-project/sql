/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class ConvertTZFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.DATE);
  }

  @Test
  public void inRangeZeroToPositive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2008-05-15 12:00:00','+00:00','+10:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2008-05-15 22:00:00"));
  }

  @Test
  public void inRangeZeroToZero() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 00:00:00','-00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 00:00:00"));
  }

  @Test
  public void inRangePositiveToPositive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 00:00:00','+10:00','+11:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 01:00:00"));
  }

  @Test
  public void inRangeNegativeToPositive() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-08:00','+09:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-13 04:34:50"));
  }

  @Test
  public void inRangeNoTZChange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','+09:00','+09:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 11:34:50"));
  }

  @Test
  public void inRangeTwentyFourHourChange() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','+12:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-13 11:34:50"));
  }

  @Test
  public void inRangeFifteenMinuteTZ() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 13:00:00','+09:30','+05:45') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 09:15:00"));
  }

  @Test
  public void nullFromFieldUnder() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-30 11:34:50','-17:00','+08:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullToFieldOver() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','+15:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullFromGarbageInput1() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','test') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullFromGarbageInput2() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021test','-12:00','+00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueFebruary() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-02-30 10:00:00','+00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueApril() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-04-31 10:00:00','+00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueMonth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-13-03 10:00:00','+00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }
}
