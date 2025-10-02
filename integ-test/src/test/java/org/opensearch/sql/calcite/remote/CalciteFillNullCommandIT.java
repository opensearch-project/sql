/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

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

  // value= syntax tests

  @Test
  public void testFillNullValueSyntaxAllFields() throws IOException {
    // fillnull value=0 (applies to all fields)
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
  public void testFillNullValueSyntaxWithFields() throws IOException {
    // fillnull value=-1 num0 (applies to specified field only)
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
  public void testFillNullValueSyntaxWithStringValue() throws IOException {
    // fillnull value='N/A' str2 (string replacement value)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, int0 | fillnull value='N/A' str2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 1),
        rows("two", null),
        rows("three", null),
        rows("N/A", null),
        rows("five", 7),
        rows("six", 3),
        rows("N/A", 8),
        rows("eight", null),
        rows("nine", null),
        rows("ten", 8),
        rows("eleven", 4),
        rows("twelve", 10),
        rows("N/A", null),
        rows("fourteen", 4),
        rows("fifteen", 11),
        rows("sixteen", 4),
        rows("N/A", 8));
  }

  // Type restriction error tests - documents error messages when type mismatches occur

  @Test
  public void testFillNullWithMixedTypeFieldsError() {
    // fillnull value=0 on mixed type fields without explicit field list
    // When no fields specified, all fields must be same type
    Throwable t =
        assertThrowsWithReplace(
            RuntimeException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields str2, int0 | fillnull value=0", TEST_INDEX_CALCS)));
    verifyErrorMessageContains(
        t,
        "fillnull failed: replacement value type INTEGER is not compatible with field 'str2' "
            + "(type: VARCHAR). The replacement value type must match the field type.");
  }

  @Test
  public void testFillNullWithStringOnNumericAndStringMixedFields() {
    // fillnull value='test' applied to fields of different types
    // Trying to fill both string and numeric fields with a string value
    Throwable t =
        assertThrowsWithReplace(
            RuntimeException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields num0, str2 | fillnull value='test' num0 str2",
                        TEST_INDEX_CALCS)));

    System.out.println("Debugging error message: " + t);
    verifyErrorMessageContains(
        t,
        "fillnull failed: replacement value type VARCHAR is not compatible with field 'num0' "
            + "(type: DOUBLE). The replacement value type must match the field type.");
  }
}
