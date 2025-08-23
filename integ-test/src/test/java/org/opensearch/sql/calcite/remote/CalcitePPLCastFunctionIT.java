/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.ppl.CastFunctionIT;

public class CalcitePPLCastFunctionIT extends CastFunctionIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testCastToUnsupportedType() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(name as boolean) | fields a", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("a", "boolean"));
    verifyDataRows(
        actual, rows((Object) null), rows((Object) null), rows((Object) null), rows((Object) null));
  }

  @Test
  public void testCastLiteralToBoolean() throws IOException {
    // In OpenSearch V2:
    // POST /_plugins/_ppl
    // {
    //   "query" : """
    //   source = opensearch_dashboards_sample_data_flights
    //   | eval a = cast(1 as boolean) -- true
    //   | eval b = cast(2 as boolean) -- true
    //   | eval c = cast(0 as boolean) -- false
    //   | eval d = cast('1' as boolean) -- false
    //   | eval e = cast('2' as boolean) -- false
    //   | eval f = cast('0' as boolean) -- false
    //   | eval g = cast('aa' as boolean) -- false
    //   | fields a,b,c,d,e,f,g | head 1
    //   """
    // }
    // But in Spark and Postgres
    // > select cast(1 as boolean); -- true
    // > select cast(2 as boolean); -- true
    // > select cast(0 as boolean); -- false
    // > select cast('1' as boolean); -- true
    // > select cast('2' as boolean); -- null
    // > select cast('0' as boolean);  -- false
    // > select cast('aa' as boolean); -- null;
    JSONObject actual;
    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(1 as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(true));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(2 as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(true));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(0 as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(false));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('1' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(true));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('2' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(actual, schema("a", "boolean"));
    verifyDataRows(actual, rows((Object) null));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('0' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(false));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('aa' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(actual, schema("a", "boolean"));
    verifyDataRows(actual, rows((Object) null));
  }

  @Test
  public void testCastBOOLEAN() throws IOException {
    JSONObject actual;
    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(boolean_value as INT) | fields a",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(boolean_value as LONG) | fields a",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(actual, rows(1));

    // TODO the calcite codegen cannot handle this case. The generated java code is
    // return ((Number)org.apache.calcite.linq4j.tree.Primitive.of(float.class)
    //
    // .numberValueRoundDown((org.apache.calcite.runtime.SqlFunctions.toBoolean(input_value)))).floatValue();
    // No applicable constructor/method found for actual parameters "boolean"; candidates are:
    // "public java.lang.Object
    // org.apache.calcite.linq4j.tree.Primitive.numberValueRoundDown(java.lang.Number)"
    //    actual =
    //        executeQuery(
    //            String.format(
    //                "source=%s | eval a = cast(boolean_value as FLOAT) | fields a",
    // TEST_INDEX_DATATYPE_NONNUMERIC));
    //    verifyDataRows(actual, rows(1));
    //
    //    actual =
    //        executeQuery(
    //            String.format(
    //                "source=%s | eval a = cast(boolean_value as DOUBLE) | fields a",
    // TEST_INDEX_DATATYPE_NONNUMERIC));
    //    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(boolean_value as STRING) | fields a",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(actual, rows("TRUE"));
  }

  @Test
  public void testCastIntegerToIp() {
    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval a = cast(1 as ip) | fields a",
                        TEST_INDEX_DATATYPE_NUMERIC)));
    verifyErrorMessageContains(
        t, "Cannot convert INTEGER to IP, only STRING and IP types are supported");
  }

  // Not available in v2
  @Test
  public void testCastIpToString() throws IOException {
    // Test casting ip to string
    var actual =
        executeQuery(
            String.format(
                "source=%s | eval s = cast(host as STRING) | fields s", TEST_INDEX_WEBLOGS));
    verifySchema(actual, schema("s", "string"));
    verifyDataRows(
        actual,
        rows("::1"),
        rows("0.0.0.2"),
        rows("::3"),
        rows("1.2.3.4"),
        rows("1.2.3.5"),
        rows("::ffff:1234"));
  }

  @Test
  public void testCastDate() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('1984-04-12' as DATE) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "date"));
    verifyDataRows(actual, rows("1984-04-12"), rows("1984-04-12"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval a = cast('2023-10-01 12:00:00' as date) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "date"));
    verifyDataRows(actual, rows("2023-10-01"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval a = cast('09:07:42' as DATE) | fields a",
                        TEST_INDEX_DATE_FORMATS)));

    verifyErrorMessageContains(t, "date:09:07:42 in unsupported format, please use 'yyyy-MM-dd'");
  }

  @Test
  public void testCastTime() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('09:07:42' as TIME) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("09:07:42"), rows("09:07:42"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval a = cast('09:07:42.12345' as TIME) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("09:07:42.12345"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval a = cast('1985-10-09 12:00:00' as time) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "time"));
    verifyDataRows(actual, rows("12:00:00"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval a = cast('1984-04-12' as TIME) | fields a",
                        TEST_INDEX_DATE_FORMATS)));

    verifyErrorMessageContains(
        t, "time:1984-04-12 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'");
  }

  @Test
  public void testCastTimestamp() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('1984-04-12 09:07:42' as TIMESTAMP) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("1984-04-12 09:07:42"), rows("1984-04-12 09:07:42"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval a = cast('2023-10-01 12:00:00.123456' as timestamp) |"
                    + " fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("2023-10-01 12:00:00.123456"));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('1984-04-12' as TIMESTAMP) | fields a",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("a", "timestamp"));
    verifyDataRows(actual, rows("1984-04-12 00:00:00"), rows("1984-04-12 00:00:00"));

    Throwable t =
        assertThrowsWithReplace(
            ExpressionEvaluationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval a = cast('09:07:42' as TIMESTAMP) | fields a",
                        TEST_INDEX_DATE_FORMATS)));

    verifyErrorMessageContains(
        t,
        "timestamp:09:07:42 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'");
  }
}
