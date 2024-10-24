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
}
