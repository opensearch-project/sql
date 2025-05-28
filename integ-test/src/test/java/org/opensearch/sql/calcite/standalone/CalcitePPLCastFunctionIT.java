/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class CalcitePPLCastFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
  }

  @Test
  public void testCast() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(age as string) | fields a", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastOverriding() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval age = cast(age as STRING) | fields age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("age", "string"));

    verifyDataRows(actual, rows("70"), rows("30"), rows("25"), rows("20"));
  }

  @Test
  public void testCastToUnknownType() {
    assertThrows(
        SyntaxCheckException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | eval age = cast(age as VARCHAR) | fields age",
                    TEST_INDEX_STATE_COUNTRY)));
  }

  @Test
  public void testChainedCast() {
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
  public void testCastNullValues() {
    String actual =
        execute(
            String.format(
                "source=%s | eval a = cast(state as string) | fields a",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"California\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"New York\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Ontario\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Quebec\"\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Test
  public void testCastToUnsupportedType() {
    String actual =
        execute(
            String.format(
                "source=%s | eval a = cast(name as boolean) | fields a", TEST_INDEX_STATE_COUNTRY));

    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }

  @Test
  public void testCastLiteralToBoolean() {
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

    String actualString =
        execute(
            String.format(
                "source=%s | eval a = cast('2' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actualString);

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast('0' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(false));

    actualString =
        execute(
            String.format(
                "source=%s | eval a = cast('aa' as boolean) | fields a | head 1",
                TEST_INDEX_DATATYPE_NUMERIC));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"a\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actualString);
  }

  @Test
  public void testCastINT() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(integer_number as INTEGER) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(integer_number as FLOAT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(integer_number as LONG) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(integer_number as DOUBLE) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(integer_number as STRING) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows("2"));
  }

  @Test
  public void testCastLONG() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(long_number as INT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(long_number as FLOAT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(long_number as DOUBLE) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(long_number as STRING) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows("1"));
  }

  @Test
  public void testCastFLOAT() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(float_number as INT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(6));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(float_number as LONG) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(6));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(float_number as DOUBLE) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(6.199999809265137));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(float_number as STRING) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows("6.2"));
  }

  @Test
  public void testCastDOUBLE() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(double_number as INT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(5));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(double_number as LONG) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(5));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(double_number as FLOAT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(5.1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(double_number as STRING) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows("5.1"));
  }

  @Test
  public void testCastBOOLEAN() {
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
  public void testCastNumericSTRING() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(cast(integer_number as STRING) as INT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(cast(long_number as STRING) as LONG) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(cast(float_number as STRING) as FLOAT) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(6.2));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(cast(double_number as STRING) as DOUBLE) | fields a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(actual, rows(5.1));

    actual =
        executeQuery(
            String.format(
                "source=%s | eval a = cast(cast(boolean_value as STRING) as BOOLEAN)| fields a",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifyDataRows(actual, rows(true));
  }
}
