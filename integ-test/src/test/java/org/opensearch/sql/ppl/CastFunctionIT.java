/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;

public class CastFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.DATATYPE_NUMERIC);
    loadIndex(Index.DATATYPE_NONNUMERIC);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.WEBLOGS);
  }

  @Test
  public void testCast() throws IOException {
    JSONObject actual =
        executeQuery(Index.STATE_COUNTRY.ppl("eval a = cast(age as string) | fields a"));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastOverriding() throws IOException {
    JSONObject actual =
        executeQuery(Index.STATE_COUNTRY.ppl("eval age = cast(age as STRING) | fields age"));

    verifySchema(actual, schema("age", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastToUnknownType() {
    assertThrowsWithReplace(
        SyntaxCheckException.class,
        () ->
            executeQuery(Index.STATE_COUNTRY.ppl("eval age = cast(age as VARCHAR) | fields age")));
  }

  @Test
  public void testChainedCast() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval age = cast(concat(cast(age as string), '0') as DOUBLE) | fields"
                    + " age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("age", "double"));

    verifyDataRows(actual, rows(700.0), rows(300.0), rows(250.0), rows(200.0));
  }

  @Test
  public void testCastNullValues() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.STATE_COUNTRY_WITH_NULL.ppl("eval a = cast(state as string) | fields a"));

    verifySchema(actual, schema("a", "string"));
    verifyDataRows(
        actual,
        rows("California"),
        rows("New York"),
        rows("Ontario"),
        rows("Quebec"),
        rows((Object) null),
        rows((Object) null));
  }

  @Test
  public void testCastINT() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(integer_number as INTEGER) | fields a"));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(integer_number as FLOAT) | fields a"));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(integer_number as LONG) | fields a"));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(integer_number as DOUBLE) | fields a"));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(integer_number as STRING) | fields a"));
    verifyDataRows(actual, rows("2"));
  }

  @Test
  public void testCastLONG() throws IOException {
    JSONObject actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(long_number as INT) | fields a"));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(long_number as FLOAT) | fields a"));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(long_number as DOUBLE) | fields a"));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(long_number as STRING) | fields a"));
    verifyDataRows(actual, rows("1"));
  }

  @Test
  public void testCastFLOAT() throws IOException {
    JSONObject actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(float_number as INT) | fields a"));
    verifyDataRows(actual, rows(6));

    actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(float_number as LONG) | fields a"));
    verifyDataRows(actual, rows(6));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(float_number as DOUBLE) | fields a"));
    verifyDataRows(actual, rows(6.199999809265137));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(float_number as STRING) | fields a"));
    verifyDataRows(actual, rows("6.2"));
  }

  @Test
  public void testCastDOUBLE() throws IOException {
    JSONObject actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(double_number as INT) | fields a"));
    verifyDataRows(actual, rows(5));

    actual =
        executeQuery(Index.DATATYPE_NUMERIC.ppl("eval a = cast(double_number as LONG) | fields a"));
    verifyDataRows(actual, rows(5));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(double_number as FLOAT) | fields a"));
    verifyDataRows(actual, rows(5.1));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl("eval a = cast(double_number as STRING) | fields a"));
    verifyDataRows(actual, rows("5.1"));
  }

  @Test
  public void testCastNumericSTRING() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl(
                "eval a = cast(cast(integer_number as STRING) as INT) | fields a"));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl(
                "eval a = cast(cast(long_number as STRING) as LONG) | fields a"));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl(
                "eval a = cast(cast(float_number as STRING) as FLOAT) | fields a"));
    verifyDataRows(actual, rows(6.2));

    actual =
        executeQuery(
            Index.DATATYPE_NUMERIC.ppl(
                "eval a = cast(cast(double_number as STRING) as DOUBLE) | fields a"));
    verifyDataRows(actual, rows(5.1));

    actual =
        executeQuery(
            Index.DATATYPE_NONNUMERIC.ppl(
                "eval a = cast(cast(boolean_value as STRING) as BOOLEAN)| fields a"));
    verifyDataRows(actual, rows(true));
  }

  @Test
  public void testCastDecimal() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DATATYPE_NONNUMERIC.ppl(
                "eval s = cast(0.99 as string), f = cast(12.9 as float), "
                    + "d = cast(100.00 as double), i = cast(2.9 as int) "
                    + "| fields s, f, d, i"));
    verifySchema(
        actual,
        schema("s", "string"),
        schema("f", "float"),
        schema("d", "double"),
        schema("i", "int"));
    verifyDataRows(actual, rows("0.99", 12.9f, 100.0d, 2));
  }

  @Test
  public void testCastDate() throws IOException {
    JSONObject actual =
        executeQuery(Index.DATE_FORMATS.ppl("eval a = cast('1984-04-12' as DATE) | fields a"));
    verifySchema(actual, schema("a", "date"));
    verifyDataRows(actual, rows("1984-04-12"), rows("1984-04-12"));

    actual =
        executeQuery(
            Index.DATE_FORMATS.ppl(
                "head 1 | eval a = cast('2023-10-01 12:00:00' as date) | fields a"));
    verifySchema(actual, schema("a", "date"));
    verifyDataRows(actual, rows("2023-10-01"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    Index.DATE_FORMATS.ppl("eval a = cast('09:07:42' as DATE) | fields a")));

    verifyErrorMessageContains(t, "date:09:07:42 in unsupported format, please use 'yyyy-MM-dd'");
  }

  @Test
  public void testCastTime() throws IOException {
    JSONObject actual =
        executeQuery(Index.DATE_FORMATS.ppl("eval a = cast('09:07:42' as TIME) | fields a"));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("09:07:42"), rows("09:07:42"));

    actual =
        executeQuery(
            Index.DATE_FORMATS.ppl("head 1 | eval a = cast('09:07:42.12345' as TIME) | fields a"));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("09:07:42.12345"));

    actual =
        executeQuery(
            Index.DATE_FORMATS.ppl(
                "head 1 | eval a = cast('1985-10-09 12:00:00' as time) | fields a"));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("12:00:00"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    Index.DATE_FORMATS.ppl("eval a = cast('1984-04-12' as TIME) | fields a")));

    verifyErrorMessageContains(
        t, "time:1984-04-12 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'");
  }

  @Test
  public void testCastTimestamp() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.DATE_FORMATS.ppl("eval a = cast('1984-04-12 09:07:42' as TIMESTAMP) | fields a"));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("1984-04-12 09:07:42"), rows("1984-04-12 09:07:42"));

    actual =
        executeQuery(
            Index.DATE_FORMATS.ppl(
                "head 1 | eval a = cast('2023-10-01 12:00:00.123456' as timestamp) |"
                    + " fields a"));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("2023-10-01 12:00:00.123456"));

    actual =
        executeQuery(Index.DATE_FORMATS.ppl("eval a = cast('1984-04-12' as TIMESTAMP) | fields a"));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("1984-04-12 00:00:00"), rows("1984-04-12 00:00:00"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    Index.DATE_FORMATS.ppl("eval a = cast('09:07:42' as TIMESTAMP) | fields a")));

    verifyErrorMessageContains(
        t,
        "timestamp:09:07:42 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'");
  }

  @Test
  public void testCastToIP() throws IOException {
    // Test casting IP to IP type
    JSONObject actual = executeQuery(Index.WEBLOGS.ppl("eval a = cast(host as IP) | fields a"));
    verifySchema(actual, schema("a", "ip"));
    verifyDataRows(
        actual,
        rows("::1"),
        rows("0.0.0.2"),
        rows("::3"),
        rows("1.2.3.4"),
        rows("1.2.3.5"),
        rows("::ffff:1234"));

    // Test casting valid IP literal to IP type
    actual =
        executeQuery(Index.WEBLOGS.ppl("head 1 | eval a = cast('192.168.1.1' as IP) | fields a"));
    verifySchema(actual, schema("a", "ip"));
    verifyDataRows(actual, rows("192.168.1.1"));

    // Test casting IPv6 literal to IP type
    actual =
        executeQuery(
            Index.WEBLOGS.ppl(
                "head 1 | eval a = cast('2001:0db8:85a3:0000:0000:8a2e:0370:7334' as"
                    + " IP) | fields a"));
    verifySchema(actual, schema("a", "ip"));
    verifyDataRows(actual, rows("2001:db8:85a3::8a2e:370:7334"));

    // Test casting invalid IP string to IP type
    Throwable t =
        assertThrowsWithReplace(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    Index.WEBLOGS.ppl("head 1 | eval a = cast('invalid_ip' as IP) | fields a")));
    verifyErrorMessageContains(
        t,
        "IP address string 'invalid_ip' is not valid. Error details: invalid_ip IP Address error:"
            + " validation options do not allow you to specify a non-segmented single value");
  }

  @Test
  public void testCastDoubleAsString() throws IOException {
    JSONObject actual =
        executeQuery(
            Index.STATE_COUNTRY.ppl(
                "head 1 | eval d = cast(0 as double) | eval s = cast(d as string) |"
                    + " fields s"));
    verifySchema(actual, schema("s", "string"));
    verifyDataRows(actual, rows("0.0"));
  }
}
