/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class DateTimeFormatsIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void testReadingDateFormats() throws IOException {
    String query =
        String.format(
            "SELECT weekyear_week_day, hour_minute_second_millis,"
                + " strict_ordinal_date_time FROM %s LIMIT 1",
            TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("weekyear_week_day", null, "date"),
        schema("hour_minute_second_millis", null, "time"),
        schema("strict_ordinal_date_time", null, "timestamp"));
    verifyDataRows(result, rows("1984-04-12", "09:07:42", "1984-04-12 09:07:42.000123456"));
  }

  @Test
  public void testDateFormatsWithOr() throws IOException {
    String query =
        String.format("SELECT yyyy-MM-dd_OR_epoch_millis FROM %s", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("1984-04-12 00:00:00"), rows("1984-04-12 09:07:42.000123456"));
  }

  @Test
  @SneakyThrows
  public void testCustomFormats() {
    String query =
        String.format(
            "SELECT custom_time, custom_timestamp, custom_date_or_date,"
                + "custom_date_or_custom_time, custom_time_parser_check FROM %s",
            TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("custom_time", null, "time"),
        schema("custom_timestamp", null, "timestamp"),
        schema("custom_date_or_date", null, "date"),
        schema("custom_date_or_custom_time", null, "timestamp"),
        schema("custom_time_parser_check", null, "time"));
    verifyDataRows(
        result,
        rows(
            "09:07:42", "1984-04-12 09:07:42", "1984-04-12", "1961-04-12 00:00:00", "23:44:36.321"),
        rows(
            "21:07:42",
            "1984-04-12 22:07:42",
            "1984-04-12",
            "1970-01-01 09:07:00",
            "09:01:16.542"));
  }

  @Test
  @SneakyThrows
  public void testCustomFormats2() {
    String query =
        String.format(
            "SELECT custom_no_delimiter_date, custom_no_delimiter_time,"
                + "custom_no_delimiter_ts FROM %s",
            TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("custom_no_delimiter_date", null, "date"),
        schema("custom_no_delimiter_time", null, "time"),
        schema("custom_no_delimiter_ts", null, "timestamp"));
    verifyDataRows(
        result,
        rows("1984-10-20", "10:20:30", "1984-10-20 15:35:48"),
        rows("1961-04-12", "09:07:00", "1961-04-12 09:07:00"));
  }

  @Test
  @SneakyThrows
  public void testIncompleteFormats() {
    String query =
        String.format(
            "SELECT incomplete_1, incomplete_2, incorrect,"
                + "incomplete_custom_time, incomplete_custom_date FROM %s",
            TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("incomplete_1", null, "timestamp"),
        schema("incomplete_2", null, "date"),
        schema("incorrect", null, "timestamp"),
        schema("incomplete_custom_time", null, "time"),
        schema("incomplete_custom_date", null, "date"));
    verifyDataRows(
        result,
        rows("1984-01-01 00:00:00", null, null, "10:00:00", "1999-01-01"),
        rows("2012-01-01 00:00:00", null, null, "20:00:00", "3021-01-01"));
  }

  @Test
  @SneakyThrows
  public void testNumericFormats() {
    String query =
        String.format("SELECT epoch_sec, epoch_milli" + " FROM %s", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(
        result, schema("epoch_sec", null, "timestamp"), schema("epoch_milli", null, "timestamp"));
    verifyDataRows(
        result,
        rows("1970-01-01 00:00:42", "1970-01-01 00:00:00.042"),
        rows("1970-01-02 03:55:00", "1970-01-01 00:01:40.5"));
  }

  @Test
  @SneakyThrows
  public void testDateNanosWithFormats() {
    String query = String.format("SELECT hour_minute_second_OR_t_time"
        + " FROM %s", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("hour_minute_second_OR_t_time", null, "time"));
    verifyDataRows(result,
        rows("09:07:42"),
        rows("07:07:42"));
  }

  @Test
  @SneakyThrows
  public void testDateNanosWithFunctions() {
    // in memory funcs
    String query = String.format(
        "SELECT"
            + " hour_minute_second_OR_t_time > TIME '08:07:00',"
            + " hour_minute_second_OR_t_time < TIME '08:07:00',"
            + " hour_minute_second_OR_t_time = t_time_no_millis,"
            + " hour_minute_second_OR_t_time <> strict_t_time,"
            + " hour_minute_second_OR_t_time >= t_time"
            + " FROM %s",
        TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("hour_minute_second_OR_t_time > TIME '08:07:00'", null, "boolean"),
        schema("hour_minute_second_OR_t_time < TIME '08:07:00'", null, "boolean"),
        schema("hour_minute_second_OR_t_time = t_time_no_millis", null, "boolean"),
        schema("hour_minute_second_OR_t_time <> strict_t_time", null, "boolean"),
        schema("hour_minute_second_OR_t_time >= t_time", null, "boolean"));
    verifyDataRows(result,
        rows(true, false, true, false, true),
        rows(false, true, false, true, false));
    // push down
    query = String.format(
        "SELECT hour_minute_second_OR_t_time"
            + " FROM %s WHERE hour_minute_second_OR_t_time > TIME '08:07:00'",
        TEST_INDEX_DATE_FORMATS);
    result = executeQuery(query);
    verifySchema(result,
        schema("hour_minute_second_OR_t_time", null, "time"));
    verifyDataRows(result,
        rows("09:07:42"));
    query = String.format(
        "SELECT hour_minute_second_OR_t_time"
            + " FROM %s WHERE hour_minute_second_OR_t_time < TIME '08:07:00'",
        TEST_INDEX_DATE_FORMATS);
    result = executeQuery(query);
    verifySchema(result,
        schema("hour_minute_second_OR_t_time", null, "time"));
    verifyDataRows(result,
        rows("07:07:42"));
  }

  @Test
  @SneakyThrows
  public void testDateNanosOrderBy() {
    String query = String.format("SELECT hour_minute_second_OR_t_time"
        + " FROM %s ORDER BY hour_minute_second_OR_t_time ASC", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("hour_minute_second_OR_t_time", null, "time"));
    verifyDataRows(result,
        rows("07:07:42"),
        rows("09:07:42"));
  }


  @Test
  @SneakyThrows
  public void testDateNanosGroupBy() {
    String query = String.format("SELECT count(*)"
        + " FROM %s GROUP BY hour_minute_second_OR_t_time", TEST_INDEX_DATE_FORMATS);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("count(*)", null, "integer"));
    verifyDataRows(result,
        rows(1),
        rows(1));
  }

  @Test
  @SneakyThrows
  public void testDateNanosWithNanos() {
    String query = String.format("SELECT date_nanos_value"
        + " FROM %s", TEST_INDEX_DATATYPE_NONNUMERIC);
    JSONObject result = executeQuery(query);
    verifySchema(result,
        schema("date_nanos_value", null, "timestamp"));
    verifyDataRows(result,
        rows("2019-03-24 01:34:46.123456789"));
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
