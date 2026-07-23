/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteFormatCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
  }

  @Test
  public void testDefaultFormat() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number=1 | fields firstname, account_number | format");

    verifySchema(result, schema("search", null, "string"));
    verifyDataRows(result, rows("( ( account_number=\"1\" AND firstname=\"Amber JOHnny\" ) )"));
  }

  @Test
  public void testMetadataFieldIsIgnored() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number=1 | fields firstname, `_id` | format");

    verifyDataRows(result, rows("( ( firstname=\"Amber JOHnny\" ) )"));
  }

  @Test
  public void testMultivalueAndCustomDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number=1 | eval names=array(firstname, lastname) "
                + "| fields names | format mvsep='OR' '{' '[' 'AND' ']' 'OR' '}'");

    verifyDataRows(
        result, rows("{ [ ( names=\"Amber JOHnny\" OR names=\"Duke Willmington\" ) ] }"));
  }

  @Test
  public void testEmptyResultFallback() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number < 0 | fields firstname "
                + "| format emptystr='no matching data'");

    verifyDataRows(result, rows("no matching data"));
  }

  @Test
  public void testDefaultEmptyResult() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number < 0 | fields firstname | format");

    verifyDataRows(result, rows("NOT ()"));
  }

  @Test
  public void testUpstreamSortControlsRowOrder() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX_BANK
                + " | where account_number in (1, 6, 13) | fields account_number "
                + "| sort - account_number | format");

    verifyDataRows(
        result,
        rows(
            "( ( account_number=\"13\" ) OR ( account_number=\"6\" ) OR "
                + "( account_number=\"1\" ) )"));
  }
}
