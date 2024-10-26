/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

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
}
