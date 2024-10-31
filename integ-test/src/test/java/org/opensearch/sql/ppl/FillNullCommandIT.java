/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class FillNullCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.CALCS);
  }

  @Test
  public void testFillNullSameValueOneField() throws IOException {
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

  @Test
  public void testFillNullSameValueTwoFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields num0, num2 | fillnull with -1 in num0,num2", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows(12.3, 17.86),
        rows(-12.3, 16.73),
        rows(15.7, -1),
        rows(-15.7, 8.51),
        rows(3.5, 6.46),
        rows(-3.5, 8.98),
        rows(0, 11.69),
        rows(-1, 17.25),
        rows(10, -1),
        rows(-1, 11.5),
        rows(-1, 6.8),
        rows(-1, 3.79),
        rows(-1, -1),
        rows(-1, 13.04),
        rows(-1, -1),
        rows(-1, 10.98),
        rows(-1, 7.87));
  }

  @Test
  public void testFillNullVariousValuesOneField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields str2, num0 | fillnull using num0 = -1", TEST_INDEX_CALCS));
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
  public void testFillNullVariousValuesTwoFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields num0, num2 | fillnull using num0 = -1, num2 = -2",
                TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows(12.3, 17.86),
        rows(-12.3, 16.73),
        rows(15.7, -2),
        rows(-15.7, 8.51),
        rows(3.5, 6.46),
        rows(-3.5, 8.98),
        rows(0, 11.69),
        rows(-1, 17.25),
        rows(10, -2),
        rows(-1, 11.5),
        rows(-1, 6.8),
        rows(-1, 3.79),
        rows(-1, -2),
        rows(-1, 13.04),
        rows(-1, -2),
        rows(-1, 10.98),
        rows(-1, 7.87));
  }

  @Test
  public void testFillNullWithOtherField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fillnull using num0 = num1 | fields str2, num0", TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", 11.38),
        rows("nine", 10),
        rows("ten", 12.4),
        rows("eleven", 10.32),
        rows("twelve", 2.47),
        rows(null, 12.05),
        rows("fourteen", 10.37),
        rows("fifteen", 7.1),
        rows("sixteen", 16.81),
        rows(null, 7.12));
  }

  @Test
  public void testFillNullWithFunctionOnOtherField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fillnull with ceil(num1) in num0 | fields str2, num0",
                TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows(null, -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows(null, 0),
        rows("eight", 12),
        rows("nine", 10),
        rows("ten", 13),
        rows("eleven", 11),
        rows("twelve", 3),
        rows(null, 13),
        rows("fourteen", 11),
        rows("fifteen", 8),
        rows("sixteen", 17),
        rows(null, 8));
  }

  @Test
  public void testFillNullWithFunctionMultipleCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fillnull with num1 in num0 | fields str2, num0 | fillnull with"
                    + " 'unknown' in str2",
                TEST_INDEX_CALCS));
    verifyDataRows(
        result,
        rows("one", 12.3),
        rows("two", -12.3),
        rows("three", 15.7),
        rows("unknown", -15.7),
        rows("five", 3.5),
        rows("six", -3.5),
        rows("unknown", 0),
        rows("eight", 11.38),
        rows("nine", 10),
        rows("ten", 12.4),
        rows("eleven", 10.32),
        rows("twelve", 2.47),
        rows("unknown", 12.05),
        rows("fourteen", 10.37),
        rows("fifteen", 7.1),
        rows("sixteen", 16.81),
        rows("unknown", 7.12));
  }
}
