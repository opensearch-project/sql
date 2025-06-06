/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class MathematicalFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.CALCS);
  }

  @Test
  public void testAbs() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = abs(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testCeil() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = ceil(age) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testCeiling() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = ceiling(age) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testE() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = e() | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.E),
        rows(Math.E),
        rows(Math.E),
        rows(Math.E),
        rows(Math.E),
        rows(Math.E),
        rows(Math.E));
  }

  @Test
  public void testExp() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = exp(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.exp(32)),
        rows(Math.exp(36)),
        rows(Math.exp(28)),
        rows(Math.exp(33)),
        rows(Math.exp(36)),
        rows(Math.exp(39)),
        rows(Math.exp(34)));
  }

  @Test
  public void testFloor() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = floor(age) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testLn() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = ln(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.log(32)),
        rows(Math.log(36)),
        rows(Math.log(28)),
        rows(Math.log(33)),
        rows(Math.log(36)),
        rows(Math.log(39)),
        rows(Math.log(34)));
  }

  @Test
  public void testLogOneArg() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = log(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.log(28)),
        rows(Math.log(32)),
        rows(Math.log(33)),
        rows(Math.log(34)),
        rows(Math.log(36)),
        rows(Math.log(36)),
        rows(Math.log(39)));
  }

  @Test
  public void testLogTwoArgs() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = log(age, balance) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        closeTo(Math.log(39225) / Math.log(32)),
        closeTo(Math.log(5686) / Math.log(36)),
        closeTo(Math.log(32838) / Math.log(28)),
        closeTo(Math.log(4180) / Math.log(33)),
        closeTo(Math.log(16418) / Math.log(36)),
        closeTo(Math.log(40540) / Math.log(39)),
        closeTo(Math.log(48086) / Math.log(34)));
  }

  @Test
  public void testLog10() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = log10(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        closeTo(Math.log10(32)),
        closeTo(Math.log10(36)),
        closeTo(Math.log10(28)),
        closeTo(Math.log10(33)),
        closeTo(Math.log10(36)),
        closeTo(Math.log10(39)),
        closeTo(Math.log10(34)));
  }

  @Test
  public void testLog2() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = log2(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        closeTo(Math.log(32) / Math.log(2)),
        closeTo(Math.log(36) / Math.log(2)),
        closeTo(Math.log(28) / Math.log(2)),
        closeTo(Math.log(33) / Math.log(2)),
        closeTo(Math.log(36) / Math.log(2)),
        closeTo(Math.log(39) / Math.log(2)),
        closeTo(Math.log(34) / Math.log(2)));
  }

  @Test
  public void testConv() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = conv(age, 10, 16) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "string"));
    verifyDataRows(
        result, rows("20"), rows("24"), rows("1c"), rows("21"), rows("24"), rows("27"), rows("22"));
  }

  @Test
  public void testCrc32() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = crc32(firstname) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "bigint"));
    verifyDataRows(
        result,
        rows(324249283),
        rows(3369714977L),
        rows(1165568529),
        rows(2293694493L),
        rows(3936131563L),
        rows(256963594),
        rows(824319315));
  }

  @Test
  public void testMod() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = mod(age, 10) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(2), rows(6), rows(8), rows(3), rows(6), rows(9), rows(4));
  }

  @Test
  public void testPow() throws IOException {
    JSONObject pow =
        executeQuery(String.format("source=%s | eval f = pow(age, 2) | fields f", TEST_INDEX_BANK));
    verifySchema(pow, schema("f", null, "double"));
    verifyDataRows(
        pow,
        rows(1024.0),
        rows(1296.0),
        rows(784.0),
        rows(1089.0),
        rows(1296.0),
        rows(1521.0),
        rows(1156.0));

    JSONObject power =
        executeQuery(
            String.format("source=%s | eval f = power(age, 2) | fields f", TEST_INDEX_BANK));
    verifySchema(power, schema("f", null, "double"));
    verifyDataRows(
        power,
        rows(1024.0),
        rows(1296.0),
        rows(784.0),
        rows(1089.0),
        rows(1296.0),
        rows(1521.0),
        rows(1156.0));
  }

  @Test
  public void testRound() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = round(age) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));

    result =
        executeQuery(
            String.format("source=%s | eval f = round(age, -1) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(30), rows(40), rows(30), rows(30), rows(40), rows(40), rows(30));
  }

  @Test
  public void testSign() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = sign(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(1), rows(1), rows(1), rows(1), rows(1), rows(1), rows(1));
  }

  @Test
  public void testSqrt() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = sqrt(age) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(5.656854249492381),
        rows(6.0),
        rows(5.291502622129181),
        rows(5.744562646538029),
        rows(6.0),
        rows(6.244997998398398),
        rows(5.830951894845301));
  }

  @Test
  public void testCbrt() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = cbrt(num3) | fields f", TEST_INDEX_CALCS));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        closeTo(Math.cbrt(-11.52)),
        closeTo(Math.cbrt(-9.31)),
        closeTo(Math.cbrt(-12.17)),
        closeTo(Math.cbrt(-7.25)),
        closeTo(Math.cbrt(12.93)),
        closeTo(Math.cbrt(-19.96)),
        closeTo(Math.cbrt(10.93)),
        closeTo(Math.cbrt(3.64)),
        closeTo(Math.cbrt(-13.38)),
        closeTo(Math.cbrt(-10.56)),
        closeTo(Math.cbrt(-4.79)),
        closeTo(Math.cbrt(-10.81)),
        closeTo(Math.cbrt(-6.62)),
        closeTo(Math.cbrt(-18.43)),
        closeTo(Math.cbrt(6.84)),
        closeTo(Math.cbrt(-10.98)),
        closeTo(Math.cbrt(-2.6)));
  }

  @Test
  public void testTruncate() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = truncate(age, 1) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));

    result =
        executeQuery(
            String.format("source=%s | eval f = truncate(age, -1) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(30), rows(30), rows(20), rows(30), rows(30), rows(30), rows(30));
  }

  @Test
  public void testPi() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = pi() | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.PI),
        rows(Math.PI),
        rows(Math.PI),
        rows(Math.PI),
        rows(Math.PI),
        rows(Math.PI),
        rows(Math.PI));
  }

  @Test
  public void testRand() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = rand() | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "double"));
    } else {
      verifySchema(result, schema("f", null, "float"));
    }
    result =
        executeQuery(String.format("source=%s | eval f = rand(5) | fields f", TEST_INDEX_BANK));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "double"));
    } else {
      verifySchema(result, schema("f", null, "float"));
    }
  }

  @Test
  public void testAcos() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = acos(0) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.acos(0)));
  }

  @Test
  public void testAsin() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = asin(1) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.asin(1)));
  }

  @Test
  public void testAtan() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = atan(2) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan(2)));

    result =
        executeQuery(String.format("source=%s | eval f = atan(2, 3) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan2(2, 3)));
  }

  @Test
  public void testAtan2() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = atan2(2, 3) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan2(2, 3)));
  }

  @Test
  public void testCos() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = cos(1.57) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.cos(1.57)));
  }

  @Test
  public void testCot() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = cot(2) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), closeTo(1 / Math.tan(2)));
  }

  @Test
  public void testDegrees() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = degrees(1.57) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.toDegrees(1.57)));
  }

  @Test
  public void testRadians() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = radians(90) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.toRadians(90)));
  }

  @Test
  public void testSin() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = sin(1.57) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.sin(1.57)));
  }
}
