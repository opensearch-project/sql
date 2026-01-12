/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration tests for the PPL convert command with Calcite enabled. */
public class CalciteConvertCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    enableCalcite();
  }

  @Test
  public void testConvertAutoFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | fields balance | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyNumOfRows(result, 3);
    verifyDataRows(result, rows(39225.0), rows(5686.0), rows(32838.0));
  }

  @Test
  public void testConvertAutoWithStringField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval test_field = '42' | convert auto(test_field) |"
                    + " fields test_field | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("test_field", null, "double"));
    verifyDataRows(result, rows(42));
  }

  @Test
  public void testConvertNumFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert num(balance) | fields balance | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"));
    verifyDataRows(result, rows(39225.0), rows(5686.0), rows(32838.0));
  }

  @Test
  public void testConvertWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) AS balance_num | fields balance_num |"
                    + " head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance_num", null, "double"));
    verifyDataRows(result, rows(39225.0), rows(5686.0), rows(32838.0));
  }

  @Test
  public void testConvertMultipleFunctions() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance), num(age) | fields balance, age | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"), schema("age", null, "double"));
    verifyDataRows(result, rows(39225.0, 32.0), rows(5686.0, 36.0), rows(32838.0, 28.0));
  }

  @Test
  public void testConvertRmcommaFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval amount = '1,234,567.89' | convert rmcomma(amount) |"
                    + " fields amount | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("amount", "string"));
    verifyDataRows(result, rows("1234567.89"));
  }

  @Test
  public void testConvertRmunitFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval distance = '100km' | convert rmunit(distance) |"
                    + " fields distance | head 1",
                TEST_INDEX_BANK));
    verifySchema(result, schema("distance", null, "bigint"));
    verifyDataRows(result, rows(100));
  }

  @Test
  public void testConvertNoneFunction() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert none(account_number) | fields account_number | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", null, "bigint"));
    verifyDataRows(result, rows(1), rows(6), rows(13));
  }

  @Test
  public void testConvertWithWhere() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where age > 30 | convert auto(balance) | fields balance, age |"
                    + " head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("balance", null, "double"), schema("age", null, "int"));
    verifyNumOfRows(result, 3);
  }

  @Test
  public void testConvertWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | stats avg(balance) by gender",
                TEST_INDEX_BANK));
    verifySchema(result, schema("avg(balance)", null, "double"), schema("gender", "string"));
    verifyNumOfRows(result, 2);
  }
}
