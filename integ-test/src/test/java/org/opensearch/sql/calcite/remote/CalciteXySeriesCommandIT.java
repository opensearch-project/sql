/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteXySeriesCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void testXyseriesBasicSingleDataField() throws IOException {
    // Pivot gender values into columns for balance aggregation
    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "source=%s | stats avg(balance) as avg_balance by gender, state"
                        + " | xyseries state gender in ('F', 'M') avg_balance",
                    TEST_INDEX_BANK)));
    verifySchema(
        result,
        schema("state", "string"),
        schema("avg_balance: F", "double"),
        schema("avg_balance: M", "double"));
    // Ordered by state (x-field)
    verifyDataRowsInOrder(
        result,
        rows("IL", null, 39225.0),
        rows("IN", 48086.0, null),
        rows("MD", null, 4180.0),
        rows("PA", 40540.0, null),
        rows("TN", null, 5686.0),
        rows("VA", 32838.0, null),
        rows("WA", null, 16418.0));
  }

  @Test
  public void testXyseriesMultipleDataFields() throws IOException {
    // Pivot with multiple data fields

    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "source=%s | stats avg(balance) as avg_balance, count() as cnt by gender, state"
                        + " | xyseries state gender in (\"F\", \"M\") avg_balance, cnt",
                    TEST_INDEX_BANK)));
    verifySchema(
        result,
        schema("state", "string"),
        schema("avg_balance: F", "double"),
        schema("avg_balance: M", "double"),
        schema("cnt: F", "bigint"),
        schema("cnt: M", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("IL", null, 39225.0, null, 1),
        rows("IN", 48086.0, null, 1, null),
        rows("MD", null, 4180.0, null, 1),
        rows("PA", 40540.0, null, 1, null),
        rows("TN", null, 5686.0, null, 1),
        rows("VA", 32838.0, null, 1, null),
        rows("WA", null, 16418.0, null, 1));
  }

  @Test
  public void testXyseriesWithCustomSeparator() throws IOException {
    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "source=%s | stats avg(balance) as avg_balance by gender, state"
                        + " | xyseries sep=\"-\" state gender in (\"F\", \"M\") avg_balance",
                    TEST_INDEX_BANK)));
    verifySchema(
        result,
        schema("state", "string"),
        schema("avg_balance-F", "double"),
        schema("avg_balance-M", "double"));
    verifyDataRowsInOrder(
        result,
        rows("IL", null, 39225.0),
        rows("IN", 48086.0, null),
        rows("MD", null, 4180.0),
        rows("PA", 40540.0, null),
        rows("TN", null, 5686.0),
        rows("VA", 32838.0, null),
        rows("WA", null, 16418.0));
  }

  @Test
  public void testXyseriesWithFormatTemplate() throws IOException {
    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "source=%s | stats avg(balance) as avg_balance by gender, state"
                        + " | xyseries format=\"$VAL$_$AGG$\" state gender in (\"F\", \"M\")"
                        + " avg_balance",
                    TEST_INDEX_BANK)));
    verifySchema(
        result,
        schema("state", "string"),
        schema("F_avg_balance", "double"),
        schema("M_avg_balance", "double"));
    verifyDataRowsInOrder(
        result,
        rows("IL", null, 39225.0),
        rows("IN", 48086.0, null),
        rows("MD", null, 4180.0),
        rows("PA", 40540.0, null),
        rows("TN", null, 5686.0),
        rows("VA", 32838.0, null),
        rows("WA", null, 16418.0));
  }

  @Test
  public void testXyseriesPartialPivotValues() throws IOException {
    // Only pivot on "F" - rows with gender=M should produce nulls
    JSONObject result =
        executeQuery(
            StringEscapeUtils.escapeJson(
                String.format(
                    "source=%s | stats avg(balance) as avg_balance by gender, state"
                        + " | xyseries state gender in (\"F\") avg_balance",
                    TEST_INDEX_BANK)));
    verifySchema(result, schema("state", "string"), schema("avg_balance: F", "double"));
    verifyDataRowsInOrder(
        result,
        rows("IL", null),
        rows("IN", 48086.0),
        rows("MD", null),
        rows("PA", 40540.0),
        rows("TN", null),
        rows("VA", 32838.0),
        rows("WA", null));
  }
}
