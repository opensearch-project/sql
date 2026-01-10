/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for the PPL convert command when Calcite is disabled.
 *
 * <p>The convert command is a Calcite-only feature and should throw an error when Calcite is
 * disabled. These tests verify that the appropriate error messages are returned.
 *
 * <p>For tests of actual convert command functionality with Calcite enabled, see {@link
 * org.opensearch.sql.calcite.remote.CalciteConvertCommandIT}.
 */
public class ConvertCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testConvertAutoFunction() {
    verifyQueryThrowsCalciteError("source=%s | convert auto(balance) | fields balance");
  }

  @Test
  public void testConvertAutoWithMixedData() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval test_field = '42' | convert auto(test_field) | fields test_field");
  }

  @Test
  public void testConvertAutoOptimalPath() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval simple_num = '123' | convert auto(simple_num) | fields simple_num");
  }

  @Test
  public void testConvertNumFunction() {
    verifyQueryThrowsCalciteError("source=%s | convert num(balance) | fields balance");
  }

  @Test
  public void testConvertWithAlias() {
    verifyQueryThrowsCalciteError(
        "source=%s | convert auto(balance) AS balance_num | fields balance_num");
  }

  @Test
  public void testConvertMultipleFunctions() {
    verifyQueryThrowsCalciteError(
        "source=%s | convert auto(balance), num(age) | fields balance, age");
  }

  @Test
  public void testConvertRmcommaFunction() {
    verifyQueryThrowsCalciteError("source=%s | convert rmcomma(firstname) | fields firstname");
  }

  @Test
  public void testConvertNoneFunction() {
    verifyQueryThrowsCalciteError(
        "source=%s | convert none(account_number) | fields account_number");
  }

  @Test
  public void testConvertWithWhere() {
    verifyQueryThrowsCalciteError(
        "source=%s | where age > 30 | convert auto(balance) | fields balance");
  }

  @Test
  public void testConvertWithStats() {
    verifyQueryThrowsCalciteError(
        "source=%s | convert auto(balance) | stats avg(balance) by gender");
  }

  private void verifyQueryThrowsCalciteError(String query) {
    Exception e =
        assertThrows(Exception.class, () -> executeQuery(String.format(query, TEST_INDEX_BANK)));
    verifyErrorMessageContains(
        e, "Convert command is supported only when plugins.calcite.enabled=true");
  }
}
