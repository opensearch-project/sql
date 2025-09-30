/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.FillNullCommandIT;

public class CalciteFillNullCommandIT extends FillNullCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  // SPL syntax tests

  @Test
  public void testFillNullSPLDefaultAllFields() throws IOException {
    // SPL: fillnull (no params, default to 0) - only numeric fields to avoid type mismatch
    JSONObject result =
        executeQuery(String.format("source=%s | fields num0, num2 | fillnull", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows(12.3, 17.86),
        rows(-12.3, 16.73),
        rows(15.7, 0),
        rows(-15.7, 8.51),
        rows(3.5, 6.46),
        rows(-3.5, 8.98),
        rows(0, 11.69),
        rows(0, 17.25),
        rows(10, 0),
        rows(0, 11.5),
        rows(0, 6.8),
        rows(0, 3.79),
        rows(0, 0),
        rows(0, 13.04),
        rows(0, 0),
        rows(0, 10.98),
        rows(0, 7.87));
  }

  @Test
  public void testFillNullSPLValueSyntaxAllFields() throws IOException {
    // SPL: fillnull value=0 (all fields with numeric value)
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields num0, num2 | fillnull value=0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows(12.3, 17.86),
        rows(-12.3, 16.73),
        rows(15.7, 0),
        rows(-15.7, 8.51),
        rows(3.5, 6.46),
        rows(-3.5, 8.98),
        rows(0, 11.69),
        rows(0, 17.25),
        rows(10, 0),
        rows(0, 11.5),
        rows(0, 6.8),
        rows(0, 3.79),
        rows(0, 0),
        rows(0, 13.04),
        rows(0, 0),
        rows(0, 10.98),
        rows(0, 7.87));
  }

  @Test
  public void testFillNullSPLValueSyntaxWithFields() throws IOException {
    // SPL: fillnull value="unknown" host kbps
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull value=-1 num0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", -1),
        rows("nine", 10),
        rows("ten", -1),
        rows("eleven", -1),
        rows("twelve", -1),
        rows(null, -1),
        rows("fourteen", -1),
        rows("fifteen", -1),
        rows("sixteen", -1),
        rows(null, -1));
  }

  @Test
  public void testFillNullSPLDefaultWithFields() throws IOException {
    // SPL: fillnull field1 field2 (default value 0)
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields str2, num0 | fillnull num0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", 0),
        rows("nine", 10),
        rows("ten", 0),
        rows("eleven", 0),
        rows("twelve", 0),
        rows(null, 0),
        rows("fourteen", 0),
        rows("fifteen", 0),
        rows("sixteen", 0),
        rows(null, 0));
  }

  @Test
  public void testFillNullPPLStringValueOnStringField() throws IOException {
    // PPL: fillnull with "hello" in str2
    // String value on string field should work
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, int0 | fillnull with 'hello' in str2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 1),
        rows("two", null),
        rows("three", null),
        rows("hello", null), // str2 was null
        rows("five", 7),
        rows("six", 3),
        rows("hello", 8), // str2 was null
        rows("eight", null),
        rows("nine", null),
        rows("ten", 8),
        rows("eleven", 4),
        rows("twelve", 10),
        rows("hello", null), // str2 was null
        rows("fourteen", 4),
        rows("fifteen", 11),
        rows("sixteen", 4),
        rows("hello", 8)); // str2 was null
  }

  @Test
  public void testFillNullPPLStringValueOnIntegerField() {
    // PPL: fillnull with 'hello' in int0
    // PPL should fail with type mismatch error (no implicit coercion)
    try {
      executeQuery(
          String.format(
              "source=%s | fields str2, int0 | fillnull with 'hello' in int0", TEST_INDEX_CALCS));
      fail("Expected type mismatch error when applying string to integer field");
    } catch (Exception e) {
      // Expected: Calcite should throw type error
      String message = e.getMessage();
      assertTrue("Expected COALESCE error, got: " + message, message.contains("COALESCE"));
    }
  }

  @Test
  public void testFillNullPPLStringValueOnMixedFields() {
    // PPL: fillnull with 'hello' in str2, int0
    // PPL should fail due to type mismatch on int0
    try {
      executeQuery(
          String.format(
              "source=%s | fields str2, int0 | fillnull with 'hello' in str2, int0",
              TEST_INDEX_CALCS));
      fail("Expected type mismatch error when applying string to mixed field types");
    } catch (Exception e) {
      // Expected: Type error due to int0 being integer type
      String message = e.getMessage();
      System.out.println("Debugging error: " + message);
      assertTrue("Expected COALESCE error, got: " + message, message.contains("COALESCE"));
    }
  }

  @Test
  public void testFillNullBackwardCompatibilityPPLSyntax() throws IOException {
    // Ensure existing PPL syntax still works
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull with -1 in num0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", -1),
        rows("nine", 10),
        rows("ten", -1),
        rows("eleven", -1),
        rows("twelve", -1),
        rows(null, -1),
        rows("fourteen", -1),
        rows("fifteen", -1),
        rows("sixteen", -1),
        rows(null, -1));
  }
}
