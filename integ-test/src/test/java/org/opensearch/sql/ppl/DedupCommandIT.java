/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class DedupCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testDedup() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | dedup male | fields male", TEST_INDEX_BANK));
    verifyDataRows(result, rows(true), rows(false));
  }

  @Test
  public void testConsecutiveDedup() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | dedup male consecutive=true | fields male", TEST_INDEX_BANK));
    List<Object[]> actualRows = extractActualRows(result);
    List<Object[]> expectedRows = getExpectedDedupRows(actualRows);
    assertTrue("Deduplication was not consecutive", expectedRows != null);
    assertEquals(
        "Row count after deduplication does not match", expectedRows.size(), actualRows.size());

    // Verify the expected and actual rows match
    for (int i = 0; i < expectedRows.size(); i++) {
      assertArrayEquals(expectedRows.get(i), actualRows.get(i));
    }
  }

  @Test
  public void testAllowMoreDuplicates() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | dedup 2 male | fields male", TEST_INDEX_BANK));
    verifyDataRows(result, rows(true), rows(true), rows(false), rows(false));
  }

  @Test
  public void testDedupOnTextField() throws IOException {
    // `email` is mapped as text with no .keyword sub-field, so dedup runs via the text-field
    // aggregation pushdown path (composite terms + top_hits with the field read from _source).
    // Assert not just the dedup key set but also the associated projected columns per row, so
    // the top_hits round-trip is exercised end-to-end.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | dedup email | fields email, firstname, balance", TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "Amber JOHnny", 39225),
        rows("hattiebond@netagy.com", "Hattie", 5686),
        rows("nanettebates@quility.com", "Nanette", 32838),
        rows("daleadams@boink.com", "Dale", 4180),
        rows("elinorratliff@scentric.com", "Elinor", 16418),
        rows("virginiaayala@filodyne.com", "Virginia", 40540),
        rows("dillardmcpherson@quailcom.com", "Dillard", 48086));
  }

  @Test
  public void testKeepEmptyDedup() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | dedup balance keepempty=true | fields firstname, balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        result,
        rows("Amber JOHnny", 39225),
        rows("Hattie", null),
        rows("Nanette", 32838),
        rows("Dale", 4180),
        rows("Elinor", null),
        rows("Virginia", null),
        rows("Dillard", 48086));
  }

  private List<Object[]> extractActualRows(JSONObject result) {
    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> actualRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      actualRows.add(new Object[] {row.get(0)});
    }
    return actualRows;
  }

  // Create the expected deduplicated rows
  private List<Object[]> getExpectedDedupRows(List<Object[]> actualRows) {
    if (verifyConsecutiveDeduplication(actualRows)) {
      return createExpectedRows(actualRows);
    }
    return null;
  }

  // Verify consecutive deduplication
  private boolean verifyConsecutiveDeduplication(List<Object[]> actualRows) {
    Object previousValue = null;

    for (Object[] currentRow : actualRows) {
      Object currentValue = currentRow[0];
      if (previousValue != null && currentValue.equals(previousValue)) {
        return false; // If consecutive values are the same, deduplication fails
      }
      previousValue = currentValue;
    }
    return true;
  }

  // Create the expected rows after deduplication
  private List<Object[]> createExpectedRows(List<Object[]> actualRows) {
    List<Object[]> expectedRows = new ArrayList<>();
    Object previousValue = null;

    for (Object[] currentRow : actualRows) {
      Object currentValue = currentRow[0];
      if (previousValue == null || !currentValue.equals(previousValue)) {
        expectedRows.add(currentRow);
      }
      previousValue = currentValue;
    }
    return expectedRows;
  }
}
