/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = abs(age) | fields f"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testAddFunction() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("where age = add(31, 1) | fields age"));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testSubtractFunction() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("where age = subtract(33, 1) | fields age"));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testMultiplyFunction() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("where age = multiply(16, 2) | fields age"));
    verifyDataRows(result, rows(32));
  }

  @Test
  public void testDivideFunction() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("where divide(age, 2) = 16 | fields age"));
    verifyDataRows(result, rows(32), rows(33));
  }

  @Test
  public void testCeil() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = ceil(age) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testCeiling() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = ceiling(age) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testE() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = e() | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = exp(age) | fields f"));
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
  public void testExpm1() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = expm1(age) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        rows(Math.expm1(32)),
        rows(Math.expm1(36)),
        rows(Math.expm1(28)),
        rows(Math.expm1(33)),
        rows(Math.expm1(36)),
        rows(Math.expm1(39)),
        rows(Math.expm1(34)));
  }

  @Test
  public void testFloor() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = floor(age) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testLn() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = ln(age) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = log(age) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = log(age, balance) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = log10(age) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = log2(age) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = conv(age, 10, 16) | fields f"));
    verifySchema(result, schema("f", null, "string"));
    verifyDataRows(
        result, rows("20"), rows("24"), rows("1c"), rows("21"), rows("24"), rows("27"), rows("22"));
  }

  @Test
  public void testCrc32() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = crc32(firstname) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = mod(age, 10) | fields f"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(2), rows(6), rows(8), rows(3), rows(6), rows(9), rows(4));
  }

  @Test
  public void testModulus() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = modulus(age, 10) | fields f"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(2), rows(6), rows(8), rows(3), rows(6), rows(9), rows(4));
  }

  @Test
  public void testPow() throws IOException {
    JSONObject pow = executeQuery(Index.BANK.ppl("eval f = pow(age, 2) | fields f"));
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

    JSONObject power = executeQuery(Index.BANK.ppl("eval f = power(age, 2) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = round(age) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));

    result = executeQuery(Index.BANK.ppl("eval f = round(age, -1) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(30), rows(40), rows(30), rows(30), rows(40), rows(40), rows(30));
  }

  @Test
  public void testSign() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sign(age) | fields f"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(1), rows(1), rows(1), rows(1), rows(1), rows(1), rows(1));
  }

  @Test
  public void testSignum() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = signum(age) | fields f"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(1), rows(1), rows(1), rows(1), rows(1), rows(1), rows(1));
  }

  @Test
  public void testSqrt() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sqrt(age) | fields f"));
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
    JSONObject result = executeQuery(Index.CALCS.ppl("eval f = cbrt(num3) | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = truncate(age, 1) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));

    result = executeQuery(Index.BANK.ppl("eval f = truncate(age, -1) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "int"));
    } else {
      verifySchema(result, schema("f", null, "bigint"));
    }
    verifyDataRows(result, rows(30), rows(30), rows(20), rows(30), rows(30), rows(30), rows(30));
  }

  @Test
  public void testPi() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = pi() | fields f"));
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
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = rand() | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "double"));
    } else {
      verifySchema(result, schema("f", null, "float"));
    }
    result = executeQuery(Index.BANK.ppl("eval f = rand(5) | fields f"));
    if (isCalciteEnabled()) {
      verifySchema(result, schema("f", null, "double"));
    } else {
      verifySchema(result, schema("f", null, "float"));
    }
  }

  @Test
  public void testAcos() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = acos(0) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.acos(0)));
  }

  @Test
  public void testAsin() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = asin(1) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.asin(1)));
  }

  @Test
  public void testAtan() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = atan(2) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan(2)));

    result = executeQuery(Index.BANK.ppl("eval f = atan(2, 3) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan2(2, 3)));
  }

  @Test
  public void testAtan2() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = atan2(2, 3) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.atan2(2, 3)));
  }

  @Test
  public void testCos() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = cos(1.57) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.cos(1.57)));
  }

  @Test
  public void testCosh() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = cosh(1.5) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.cosh(1.5)));
  }

  @Test
  public void testCot() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = cot(2) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), closeTo(1 / Math.tan(2)));
  }

  @Test
  public void testDegrees() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = degrees(1.57) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.toDegrees(1.57)));
  }

  @Test
  public void testRadians() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = radians(90) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.toRadians(90)));
  }

  @Test
  public void testSin() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sin(1.57) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.sin(1.57)));
  }

  @Test
  public void testSinh() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sinh(1.5) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.sinh(1.5)));
  }

  @Test
  public void testRint() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = rint(1.7) | fields f"));
    verifySchema(result, schema("f", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(Math.rint(1.7)));
  }

  // SUM function tests for eval command
  @Test
  public void testEvalSumSingleInteger() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sum(42) | fields f | head 5"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(42), rows(42), rows(42), rows(42), rows(42));
  }

  @Test
  public void testEvalSumMultipleIntegers() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sum(1, 2, 3) | fields f | head 5"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(6), rows(6), rows(6), rows(6), rows(6));
  }

  @Test
  public void testEvalSumMixedNumericTypes() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sum(1, 2.5) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(3.5), rows(3.5), rows(3.5), rows(3.5), rows(3.5));
  }

  @Test
  public void testEvalSumWithFields() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = sum(age, 10) | fields f | head 7"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(42), rows(46), rows(38), rows(43), rows(46), rows(49), rows(44));
  }

  @Test
  public void testEvalSumMultipleNumericArguments() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = sum(1, 2, 3, 4, 5) | fields f | head 5"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(15), rows(15), rows(15), rows(15), rows(15));
  }

  // AVG function tests for eval command
  @Test
  public void testEvalAvgSingleInteger() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(42) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(42.0), rows(42.0), rows(42.0), rows(42.0), rows(42.0));
  }

  @Test
  public void testEvalAvgMultipleIntegers() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(1, 2, 3) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0));
  }

  @Test
  public void testEvalAvgTwoIntegers() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(1, 2) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(1.5), rows(1.5), rows(1.5), rows(1.5), rows(1.5));
  }

  @Test
  public void testEvalAvgMixedNumericTypes() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(1, 2.5) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(1.75), rows(1.75), rows(1.75), rows(1.75), rows(1.75));
  }

  @Test
  public void testEvalAvgWithFields() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(age, 10) | fields f | head 7"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(21.0), rows(23.0), rows(19.0), rows(21.5), rows(23.0), rows(24.5), rows(22.0));
  }

  @Test
  public void testEvalAvgMultipleValues() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(1, 4) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5));
  }

  @Test
  public void testEvalAvgFiveValues() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = avg(1, 2, 3, 4, 5) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(3.0), rows(3.0), rows(3.0), rows(3.0), rows(3.0));
  }

  // Combined sum and avg tests
  @Test
  public void testEvalSumAndAvgComparison() throws IOException {
    JSONObject result =
        executeQuery(
            Index.BANK.ppl(
                "eval sum_val = sum(10, 20, 30), avg_val = avg(10, 20, 30) | fields"
                    + " sum_val, avg_val | head 5"));
    verifySchema(result, schema("sum_val", null, "int"), schema("avg_val", null, "double"));
    verifyDataRows(
        result, rows(60, 20.0), rows(60, 20.0), rows(60, 20.0), rows(60, 20.0), rows(60, 20.0));
  }

  @Test
  public void testEvalSumInWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            Index.BANK.ppl("where sum(age, 10) > 40 | eval f = sum(age, 10) | fields f | head 6"));
    verifySchema(result, schema("f", null, "int"));
    // Should return rows where age + 10 > 40, so age > 30
    verifyDataRows(result, rows(42), rows(46), rows(43), rows(46), rows(49), rows(44));
  }

  @Test
  public void testEvalAvgInWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            Index.BANK.ppl("where avg(age, 10) > 20 | eval f = avg(age, 10) | fields f | head 6"));
    verifySchema(result, schema("f", null, "double"));
    // Should return rows where (age + 10) / 2 > 20, so age > 30
    verifyDataRows(result, rows(21.0), rows(23.0), rows(21.5), rows(23.0), rows(24.5), rows(22.0));
  }

  @Test
  public void testEvalComplexExpression() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = sum(age, 5) + avg(10, 20) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    // sum(age, 5) + avg(10, 20) = (age + 5) + 15.0
    verifyDataRows(result, rows(52.0), rows(56.0), rows(48.0), rows(53.0), rows(56.0));
  }

  @Test
  public void testEvalNestedSumAvg() throws IOException {
    // Note: This tests the arithmetic expression rewriting, not actual nested function calls
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = sum(avg(20, 30), 10) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    // avg(20, 30) = 25.0, sum(25.0, 10) = 35.0
    verifyDataRows(result, rows(35.0), rows(35.0), rows(35.0), rows(35.0), rows(35.0));
  }

  @Test
  public void testEvalSumWithMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = sum(age, age, 10) | fields f | head 5"));
    verifySchema(result, schema("f", null, "int"));
    // sum(age, age, 10) = age + age + 10 = 2*age + 10
    verifyDataRows(result, rows(74), rows(82), rows(66), rows(76), rows(82));
  }

  @Test
  public void testEvalAvgWithExpression() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = avg(age * 2, 10) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    // avg(age * 2, 10) = (age * 2 + 10) / 2 = age + 5
    verifyDataRows(result, rows(37.0), rows(41.0), rows(33.0), rows(38.0), rows(41.0));
  }

  @Test
  public void testEvalSumWithNegativeNumbers() throws IOException {
    JSONObject result =
        executeQuery(Index.BANK.ppl("eval f = sum(-5, 10, -3) | fields f | head 5"));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(2), rows(2), rows(2), rows(2), rows(2));
  }

  @Test
  public void testEvalAvgWithNegativeNumbers() throws IOException {
    JSONObject result = executeQuery(Index.BANK.ppl("eval f = avg(-10, 10) | fields f | head 5"));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(0.0), rows(0.0), rows(0.0), rows(0.0), rows(0.0));
  }
}
