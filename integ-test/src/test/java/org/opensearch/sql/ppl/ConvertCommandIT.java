/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/** Integration tests for the PPL convert command. */
public class ConvertCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testConvertAutoFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | fields balance", TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertNumFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert num(balance) | fields balance", TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) AS balance_num | fields balance_num",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance_num", null, "double"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertMultipleFunctions() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance), num(age) | fields balance, age",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"), schema("age", null, "double"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertRmcommaFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert rmcomma(firstname) | fields firstname",
                TEST_INDEX_BANK));
    verifySchema(result, schema("firstname", "string"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertNoneFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert none(account_number) | fields account_number",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", null, "long"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where age > 30 | convert auto(balance) | fields balance",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result);
  }

  @Test
  public void testConvertWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | stats avg(balance) by gender",
                TEST_INDEX_BANK));
    verifySchema(result, schema("avg(balance)", null, "double"), schema("gender", "string"));
    verifyDataRows(result);
  }
}
