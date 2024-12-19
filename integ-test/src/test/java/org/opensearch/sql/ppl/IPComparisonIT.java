/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class IPComparisonIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(SQLIntegTestCase.Index.WEBLOG);
  }

  @Test
  public void test_equal() throws IOException {
    JSONObject result;
    final String operator = "=";

    result = executeComparisonQuery(operator, "1.2.3.4");
    verifyDataRows(result, rows("1.2.3.4"));

    result = executeComparisonQuery(operator, "::ffff:1.2.3.4");
    verifyDataRows(result, rows("1.2.3.4"));

    result = executeComparisonQuery(operator, "::1");
    verifyDataRows(result, rows("::1"));

    result = executeComparisonQuery(operator, "0000:0000:0000:0000:0000:0000:0000:0001");
    verifyDataRows(result, rows("::1"));
  }

  @Test
  public void test_not_equal() throws IOException {
    JSONObject result;
    final String operator = "!=";

    result = executeComparisonQuery(operator, "1.2.3.4");
    verifyDataRows(
        result, rows("::1"), rows("0.0.0.2"), rows("::3"), rows("1.2.3.5"), rows("::ffff:1234"));

    result = executeComparisonQuery(operator, "::ffff:1.2.3.4");
    verifyDataRows(
        result, rows("::1"), rows("0.0.0.2"), rows("::3"), rows("1.2.3.5"), rows("::ffff:1234"));

    result = executeComparisonQuery(operator, "::1");
    verifyDataRows(
        result,
        rows("0.0.0.2"),
        rows("::3"),
        rows("1.2.3.4"),
        rows("1.2.3.5"),
        rows("::ffff:1234"));

    result = executeComparisonQuery(operator, "0000:0000:0000:0000:0000:0000:0000:0001");
    verifyDataRows(
        result,
        rows("0.0.0.2"),
        rows("::3"),
        rows("1.2.3.4"),
        rows("1.2.3.5"),
        rows("::ffff:1234"));
  }

  @Test
  public void test_greater_than() throws IOException {
    JSONObject result;
    final String operator = ">";

    result = executeComparisonQuery(operator, "1.2.3.3");
    verifyDataRows(result, rows("1.2.3.4"), rows("1.2.3.5"));

    result = executeComparisonQuery(operator, "1.2.3.4");
    verifyDataRows(result, rows("1.2.3.5"));

    result = executeComparisonQuery(operator, "1.2.3.5");
    verifyDataRows(result);
  }

  @Test
  public void test_greater_than_or_equal_to() throws IOException {
    JSONObject result;
    final String operator = ">=";

    result = executeComparisonQuery(operator, "1.2.3.4");
    verifyDataRows(result, rows("1.2.3.4"), rows("1.2.3.5"));

    result = executeComparisonQuery(operator, "1.2.3.5");
    verifyDataRows(result, rows("1.2.3.5"));

    result = executeComparisonQuery(operator, "1.2.3.6");
    verifyDataRows(result);
  }

  @Test
  public void test_less_than() throws IOException {
    JSONObject result;
    final String operator = "<";

    result = executeComparisonQuery(operator, "::4");
    verifyDataRows(result, rows("::1"), rows("::3"));

    result = executeComparisonQuery(operator, "::3");
    verifyDataRows(result, rows("::1"));

    result = executeComparisonQuery(operator, "::1");
    verifyDataRows(result);
  }

  @Test
  public void test_less_than_or_equal_to() throws IOException {
    JSONObject result;
    final String operator = "<=";

    result = executeComparisonQuery(operator, "::3");
    verifyDataRows(result, rows("::1"), rows("::3"));

    result = executeComparisonQuery(operator, "::1");
    verifyDataRows(result, rows("::1"));

    result = executeComparisonQuery(operator, "::0");
    verifyDataRows(result);
  }

  /**
   * Executes a query comparison on the weblogs test index with the given comparison operator and IP
   * address string, and returns the resulting {@link JSONObject};
   */
  private JSONObject executeComparisonQuery(String comparisonOperator, String addressString)
      throws IOException {
    String formatString = "source=%s | where host %s '%s' | fields host";
    String query =
        String.format(formatString, TEST_INDEX_WEBLOGS, comparisonOperator, addressString);
    return executeQuery(query);
  }
}
