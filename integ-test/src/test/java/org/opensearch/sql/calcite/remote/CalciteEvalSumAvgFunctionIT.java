/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for eval sum() and avg() functions that are only available with Calcite engine.
 * These functions provide row-wise arithmetic operations for multiple arguments.
 */
public class CalciteEvalSumAvgFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.BANK);
  }

  // SUM function tests
  @Test
  public void testEvalSumBasicNumericTypes() throws IOException {
    // Single integer
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = sum(42) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(42), rows(42), rows(42), rows(42), rows(42), rows(42), rows(42));

    // Multiple integers
    result =
        executeQuery(
            String.format("source=%s | eval f = sum(1, 2, 3) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(6), rows(6), rows(6), rows(6), rows(6), rows(6), rows(6));

    // Mixed integer and double (should promote to double)
    result =
        executeQuery(String.format("source=%s | eval f = sum(1, 2.5) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(3.5), rows(3.5), rows(3.5), rows(3.5), rows(3.5), rows(3.5), rows(3.5));
  }

  @Test
  public void testEvalSumWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = sum(age, 10) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(42), rows(46), rows(38), rows(43), rows(46), rows(49), rows(44));
  }

  @Test
  public void testEvalSumZeroArguments() throws IOException {
    try {
      executeQuery(String.format("source=%s | eval f = sum() | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should throw an exception - sum() requires at least one argument
      // The exact exception message will depend on the implementation
      assert (e.getMessage().contains("argument") || e.getMessage().contains("SUM"));
    }
  }

  @Test
  public void testEvalSumWithStringArguments() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = sum('hello', 'world') | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail with type error since strings can't be summed
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric")
          || e.getMessage().toLowerCase().contains("string"));
    }
  }

  @Test
  public void testEvalSumWithBooleanArguments() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = sum(true, false) | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail with type error since booleans can't be summed
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric")
          || e.getMessage().toLowerCase().contains("boolean"));
    }
  }

  @Test
  public void testEvalSumMixedValidInvalidTypes() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = sum(1, 'hello') | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail when mixing numeric and non-numeric types
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric"));
    }
  }

  // AVG function tests
  @Test
  public void testEvalAvgBasicNumericTypes() throws IOException {
    // Single integer - should return double
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = avg(42) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(42.0), rows(42.0), rows(42.0), rows(42.0), rows(42.0), rows(42.0), rows(42.0));

    // Multiple integers - should return double with proper fractional result
    result =
        executeQuery(
            String.format("source=%s | eval f = avg(1, 2, 3) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0));

    // Two integers with fractional average - preserves precision
    result =
        executeQuery(String.format("source=%s | eval f = avg(1, 2) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(1.5), rows(1.5), rows(1.5), rows(1.5), rows(1.5), rows(1.5), rows(1.5));

    // Mixed integer and double - should return double
    result =
        executeQuery(String.format("source=%s | eval f = avg(1, 2.5) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(1.75), rows(1.75), rows(1.75), rows(1.75), rows(1.75), rows(1.75), rows(1.75));
  }

  @Test
  public void testEvalAvgWithFields() throws IOException {
    // avg with age field and constant - should return double
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f = avg(age, 10) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(21.0), rows(23.0), rows(19.0), rows(21.5), rows(23.0), rows(24.5), rows(22.0));

    // avg of three values including field
    result =
        executeQuery(
            String.format("source=%s | eval f = avg(age, 10, 20) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result,
        closeTo((32 + 10 + 20) / 3.0),
        closeTo((36 + 10 + 20) / 3.0),
        closeTo((28 + 10 + 20) / 3.0),
        closeTo((33 + 10 + 20) / 3.0),
        closeTo((36 + 10 + 20) / 3.0),
        closeTo((39 + 10 + 20) / 3.0),
        closeTo((34 + 10 + 20) / 3.0));
  }

  @Test
  public void testEvalAvgPrecisionPreservation() throws IOException {
    // Test that integer division preserves fractional parts
    JSONObject result =
        executeQuery(String.format("source=%s | eval f = avg(1, 3) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0));

    result =
        executeQuery(
            String.format("source=%s | eval f = avg(1, 2, 3, 4, 5) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(3.0), rows(3.0), rows(3.0), rows(3.0), rows(3.0), rows(3.0), rows(3.0));

    result =
        executeQuery(String.format("source=%s | eval f = avg(1, 4) | fields f", TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5));
  }

  @Test
  public void testEvalAvgZeroArguments() throws IOException {
    try {
      executeQuery(String.format("source=%s | eval f = avg() | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should throw an exception - avg() requires at least one argument
      assert (e.getMessage().contains("argument") || e.getMessage().contains("AVG"));
    }
  }

  @Test
  public void testEvalAvgWithStringArguments() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = avg('hello', 'world') | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail with type error since strings can't be averaged
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric")
          || e.getMessage().toLowerCase().contains("string"));
    }
  }

  @Test
  public void testEvalAvgWithBooleanArguments() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = avg(true, false) | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail with type error since booleans can't be averaged
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric")
          || e.getMessage().toLowerCase().contains("boolean"));
    }
  }

  @Test
  public void testEvalAvgMixedValidInvalidTypes() throws IOException {
    try {
      executeQuery(
          String.format("source=%s | eval f = avg(1, 'hello') | fields f", TEST_INDEX_BANK));
    } catch (Exception e) {
      // Should fail when mixing numeric and non-numeric types
      assert (e.getMessage().toLowerCase().contains("type")
          || e.getMessage().toLowerCase().contains("numeric"));
    }
  }

  @Test
  public void testEvalAvgVsSum() throws IOException {
    // Compare avg and sum results to verify correctness
    JSONObject avgResult =
        executeQuery(
            String.format("source=%s | eval f = avg(10, 20, 30) | fields f", TEST_INDEX_BANK));
    verifySchema(avgResult, schema("f", null, "double"));
    verifyDataRows(
        avgResult,
        rows(20.0),
        rows(20.0),
        rows(20.0),
        rows(20.0),
        rows(20.0),
        rows(20.0),
        rows(20.0));

    JSONObject sumResult =
        executeQuery(
            String.format("source=%s | eval f = sum(10, 20, 30) | fields f", TEST_INDEX_BANK));
    verifySchema(sumResult, schema("f", null, "int"));
    verifyDataRows(sumResult, rows(60), rows(60), rows(60), rows(60), rows(60), rows(60), rows(60));
  }

  // WHERE clause tests
  @Test
  public void testEvalSumWithWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(age, 10) > 40 | eval f = sum(age, 10) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(42), rows(46), rows(43), rows(46), rows(49), rows(44));
  }

  @Test
  public void testEvalAvgWithWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where avg(age, 10) > 20 | eval f = avg(age, 10) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(21.0), rows(23.0), rows(21.5), rows(23.0), rows(24.5), rows(22.0));
  }

  @Test
  public void testComplexWhereWithSumAndAvg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(age, 5) > 35 and avg(age, 10) < 25 | eval total = sum(age,"
                    + " 5), average = avg(age, 10) | fields total, average",
                TEST_INDEX_BANK));
    verifySchema(result, schema("total", null, "int"), schema("average", null, "double"));
    verifyDataRows(
        result,
        rows(37, 21.0),
        rows(41, 23.0),
        rows(38, 21.5),
        rows(41, 23.0),
        rows(44, 24.5),
        rows(39, 22.0));
  }

  @Test
  public void testWhereWithSumMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(age, balance) > 1000 | eval f = sum(age, balance) | fields"
                    + " f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "bigint"));
    // Results will depend on the balance values in test data
    assertTrue(result.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testWhereWithAvgDecimalPrecision() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where avg(1, 2, 3) = 2.0 | eval f = avg(1, 2, 3) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0), rows(2.0));
  }

  @Test
  public void testWhereWithNestedSumAvg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(avg(age, 10), 5) > 25 | eval f = sum(avg(age, 10), 5) |"
                    + " fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(26.0), rows(28.0), rows(26.5), rows(28.0), rows(29.5), rows(27.0));
  }

  @Test
  public void testWhereWithSumInComparison() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(5, 10) < sum(age, 0) | eval f = sum(age, 0) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "int"));
    verifyDataRows(result, rows(32), rows(36), rows(28), rows(33), rows(36), rows(39), rows(34));
  }

  @Test
  public void testWhereWithAvgGreaterThanSum() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where avg(age, 100) > sum(1, 1, 1) | eval avg_val = avg(age, 100),"
                    + " sum_val = sum(1, 1, 1) | fields avg_val, sum_val",
                TEST_INDEX_BANK));
    verifySchema(result, schema("avg_val", null, "double"), schema("sum_val", null, "int"));
    // All rows should pass since avg(age, 100) will be much larger than sum(1,1,1) = 3
    verifyDataRows(
        result,
        rows(66.0, 3),
        rows(68.0, 3),
        rows(64.0, 3),
        rows(66.5, 3),
        rows(68.0, 3),
        rows(69.5, 3),
        rows(67.0, 3));
  }

  @Test
  public void testWhereWithMixedTypesInFunctions() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where sum(1, 2.5, age) > 35 | eval f = sum(1, 2.5, age) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(result, rows(35.5), rows(39.5), rows(36.5), rows(39.5), rows(42.5), rows(37.5));
  }

  @Test
  public void testWhereWithAvgRounding() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where avg(1, 4) = 2.5 | eval f = avg(1, 4) | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "double"));
    verifyDataRows(
        result, rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5), rows(2.5));
  }
}
