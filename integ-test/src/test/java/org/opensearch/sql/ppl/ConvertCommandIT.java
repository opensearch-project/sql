/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import org.junit.jupiter.api.Test;

/** Integration tests for the PPL convert command when Calcite is disabled. */
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

  @Test
  public void testConvertMktimeFunction() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval date_str = '2003-10-18 20:07:13' | convert mktime(date_str) | fields date_str");
  }

  @Test
  public void testConvertCtimeFunction() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval timestamp = 1066507633 | convert ctime(timestamp) | fields timestamp");
  }

  @Test
  public void testConvertDur2secFunction() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval duration = '01:23:45' | convert dur2sec(duration) | fields duration");
  }

  @Test
  public void testConvertMstimeFunction() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval time_str = '03:45' | convert mstime(time_str) | fields time_str");
  }

  @Test
  public void testConvertWithTimeformat() {
    verifyQueryThrowsCalciteError(
        "source=%s | eval date_str = '18/10/2003 20:07:13' | convert timeformat=\"dd/MM/yyyy HH:mm:ss\" mktime(date_str) | fields date_str");
  }

  private void verifyQueryThrowsCalciteError(String query) {
    Exception e =
        assertThrows(Exception.class, () -> executeQuery(String.format(query, TEST_INDEX_BANK)));
    verifyErrorMessageContains(e, "convert is supported only when plugins.calcite.enabled=true");
  }
}
