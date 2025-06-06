/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLTrendlineIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testTrendlineSma() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline sma(3, balance) as balance_trend |"
                    + " fields balance_trend",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance_trend", "double"));
    verifyDataRows(
        result, rows((Object) null), rows((Object) null), rows(37534.333333333336), rows(40488));
  }

  @Test
  public void testTrendlineWma() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline wma(3, balance) as balance_trend |"
                    + " fields balance_trend",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance_trend", "double"));
    verifyDataRows(
        result, rows((Object) null), rows((Object) null), rows(37753.5), rows(43029.333333333336));
  }

  @Test
  public void testTrendlineMultipleFields() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline sma(2, balance) as sma wma(3,"
                    + " balance) as wma | fields balance, sma, wma",
                TEST_INDEX_BANK));
    verifySchema(
        result, schema("balance", "long"), schema("sma", "double"), schema("wma", "double"));
    verifyDataRows(
        result,
        rows(39225, null, null),
        rows(32838, 36031.5, null),
        rows(40540, 36689, 37753.5),
        rows(48086, 44313, 43029.333333333336));
  }

  @Test
  public void testTrendlineNoAlias() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline sma(2, balance) | fields"
                    + " balance, balance_trendline",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", "long"), schema("balance_trendline", "double"));
    verifyDataRows(
        result, rows(39225, null), rows(32838, 36031.5), rows(40540, 36689), rows(48086, 44313));
  }

  @Test
  public void testTrendlineOverwritesExisingField() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline sma(2, balance) as balance | fields"
                    + " balance",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", "double"));
    verifyDataRows(result, rows((Object) null), rows(36031.5), rows(36689), rows(44313));
  }

  @Test
  public void testTrendlineWithSort() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | trendline sort - balance sma(2, balance) |"
                    + " fields balance, balance_trendline",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", "long"), schema("balance_trendline", "double"));
    verifyDataRows(
        result, rows(48086, null), rows(40540, 44313), rows(39225, 39882.5), rows(32838, 36031.5));
  }

  @Test
  public void testTrendlinePreFilterNullValues() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | trendline sma(2, balance) | fields" + " balance, balance_trendline",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(result, schema("balance", "long"), schema("balance_trendline", "double"));
    verifyDataRows(
        result, rows(39225, null), rows(32838, 36031.5), rows(4180, 18509), rows(48086, 26133));
  }
}
