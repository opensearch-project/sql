/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testSqrtAndPow() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where sqrt(pow(age, 2)) = 30.0 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testAbs() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = abs(-30) | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testAbsWithField() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where abs(age) = 30 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testAcos() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval acos = acos(1) | head 1 | fields acos",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("acos", "double"));
    verifyDataRows(actual, rows(0.0));
  }

  @Test
  public void testAsin() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval asin = asin(0.5) | head 1 | fields asin",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("asin", "double"));
    verifyDataRows(actual, closeTo(0.5235987755982988));
  }

  @Test
  public void testAtan() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = atan(0) + 30 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testAtan2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = atan2(0, 1) + 30 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testCbrt() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = cbrt(27000) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testCeiling() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = ceiling(29.7) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConv() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = conv('1E', 16, 10) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testCos() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = cos(0) + 19 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Jane", 20));
  }

  @Test
  public void testPiAndCot() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval cot = cot(pi() / 4) | head 1 | fields cot",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("cot", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testCrc32() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where crc32(name) = 1516115372 | fields name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("Jane"));
  }

  @Test
  public void testDegrees() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval degrees = degrees(0.5235987755982988) | head 1 | fields degrees",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("degrees", "double"));
    verifyDataRows(actual, closeTo(30.0));
  }

  @Test
  public void testEAndLn() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval ln_e = ln(e()) | head 1 | fields ln_e",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("ln_e", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testExpAndFloor() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = floor(exp(3.41)) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLog() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where log(age, 900) = 2 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLog2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where log2(age + 2) = 5 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLog10() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where log10(age + 30) = 2 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Jake", 70));
  }

  @Test
  public void testLn() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where ln(age) > 4 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Jake", 70));
  }

  @Test
  public void testMod() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where mod(age, 10) = 0 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Jake", 70), rows("Hello", 30), rows("Jane", 20));
  }

  @Test
  public void testRadians() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval radians = radians(30) | head 1 | fields radians",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("radians", "double"));
    verifyDataRows(actual, closeTo(0.5235987755982988));
  }

  @Test
  public void testRand() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval rand = rand() | where rand > 0 | where rand < 1  | fields name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("Jake"), rows("Hello"), rows("Jane"), rows("John"));
  }

  @Test
  public void testRound() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = round(29.7) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testSign() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = sign(-3) + 31 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testSin() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age = sin(0) + 20 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Jane", 20));
  }
}
