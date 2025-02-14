/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class TrendlineCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
  }

  @Test
  public void testTrendline() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 39000 | sort balance | trendline sma(2, balance) as"
                    + " balance_trend | fields balance_trend",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows(new Object[] {null}), rows(44313.0), rows(39882.5));
  }

  @Test
  public void testTrendlineMultipleFields() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 39000 | sort balance | trendline sma(2, balance) as"
                    + " balance_trend sma(2, account_number) as account_number_trend | fields"
                    + " balance_trend, account_number_trend",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows(null, null), rows(44313.0, 28.5), rows(39882.5, 13.0));
  }

  @Test
  public void testTrendlineOverwritesExistingField() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 39000 | sort balance | trendline sma(2, balance) as"
                    + " age | fields age",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows(new Object[] {null}), rows(44313.0), rows(39882.5));
  }

  @Test
  public void testTrendlineNoAlias() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 39000 | sort balance | trendline sma(2, balance) |"
                    + " fields balance_sma_trendline",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows(new Object[] {null}), rows(44313.0), rows(39882.5));
  }

  @Test
  public void testTrendlineWithSort() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 39000 | trendline sort balance sma(2, balance) |"
                    + " fields balance_sma_trendline",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows(new Object[] {null}), rows(44313.0), rows(39882.5));
  }

  @Test
  public void testTrendlineWma() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | head 4 | trendline wma(4, balance) as"
                    + " balance_trend | fields balance_trend",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(19615.8));
  }

  @Test
  public void testTrendlineMultipleFieldsWma() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | head 5 | trendline wma(4, balance) as"
                    + " balance_trend wma(5, account_number) as account_number_trend | fields"
                    + " balance_trend, account_number_trend",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(null, null),
        rows(null, null),
        rows(null, null),
        rows(19615.8, null),
        rows(29393.6, 9.8));
  }

  @Test
  public void testTrendlineOverwritesExistingFieldWma() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | head 6 | trendline wma(4, balance) as"
                    + " age | fields age",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(19615.8),
        rows(29393.6),
        rows(36192.9));
  }

  @Test
  public void testTrendlineNoAliasWmaDefaultName() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | head 5 | trendline wma(4, balance) |"
                    + " fields balance_wma_trendline",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance_wma_trendline", "double"));
    verifyDataRows(
        result,
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(19615.8),
        rows(29393.6));
  }

  @Test
  public void testTrendlineWithSortWma() throws IOException {
    final JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | head 5 | trendline sort balance wma(4, balance) |"
                    + " fields balance_wma_trendline",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(new Object[] {null}),
        rows(19615.8),
        rows(29393.6));
  }
  //
  //  @Test
  //  public void testTrendlineWithDefaultNameWma() throws IOException {
  //    final JSONObject result =
  //            executeQuery(
  //                    String.format(
  //                            "source=%s | where balance > 39000 | trendline sort balance wma(2,
  // balance) |"
  //                                    + " fields balance_wma_trendline",
  //                            TEST_INDEX_BANK));
  //    verifySchema(
  //            result,
  //            schema("balance_wma_trendline", "double"));
  //    verifyDataRows(
  //            result, rows(new Object[] {null}), rows(40101.666666666664),
  // rows(45570.666666666664));
  //  }
}
