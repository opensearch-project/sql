/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteRegexCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testRegexBasicStringMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | regex firstname='Amber' | fields account_number, firstname",
                TEST_INDEX_ACCOUNT));

    assertEquals(1, result.getJSONArray("datarows").length());
    assertEquals("Amber", result.getJSONArray("datarows").getJSONArray(0).get(1));
  }

  @Test
  public void testRegexPartialStringMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | regex firstname='nan' | fields account_number, firstname",
                TEST_INDEX_ACCOUNT));

    // Should match names containing "nan": Fernandez, Buchanan
    assertEquals(2, result.getJSONArray("datarows").length());
    // Verify one of the results contains "nan"
    String firstName = result.getJSONArray("datarows").getJSONArray(0).get(1).toString();
    assertTrue(firstName.contains("nan"));
  }

  @Test
  public void testRegexPatternMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | regex firstname='A.*' | fields account_number, firstname",
                TEST_INDEX_ACCOUNT));

    // Should match names starting with A - there are 66 such names in accounts.json
    assertEquals(66, result.getJSONArray("datarows").length());
    // Verify first result is a name starting with A
    assertTrue(result.getJSONArray("datarows").getJSONArray(0).get(1).toString().startsWith("A"));
  }

  @Test
  public void testRegexNegatedMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | regex firstname!='Amber' | fields account_number, firstname | head 3",
                TEST_INDEX_ACCOUNT));

    assertEquals(3, result.getJSONArray("datarows").length());
    // Verify Amber is not in results
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      assertNotEquals("Amber", result.getJSONArray("datarows").getJSONArray(i).get(1));
    }
  }

  @Test
  public void testRegexWithStateField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | regex state='CA' | fields account_number, firstname, state",
                TEST_INDEX_ACCOUNT));

    // There are 17 CA records in accounts.json
    assertEquals(17, result.getJSONArray("datarows").length());
    assertEquals("CA", result.getJSONArray("datarows").getJSONArray(0).get(2));
  }
}
