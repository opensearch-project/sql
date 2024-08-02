/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class SortCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DOG);
  }

  @Test
  public void testSortCommand() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | sort age | fields age", TEST_INDEX_BANK));
    verifyOrder(result, rows(28), rows(32), rows(33), rows(34), rows(36), rows(36), rows(39));
  }

  @Test
  public void testSortWithNullValue() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | fields firstname, balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));

    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> nullRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      if (row.isNull(1)) {
        nullRows.add(new Object[] {row.getString(0), null});
      }
    }
    // Verify the set values for null balances as rows with null balance can return in any order
    List<Object[]> expectedNullRows =
        Arrays.asList(
            new Object[] {"Hattie", null},
            new Object[] {"Elinor", null},
            new Object[] {"Virginia", null});
    assertSetEquals(expectedNullRows, nullRows);

    // Create a new JSONArray to store not null balance rows
    JSONArray nonNullBalanceRows = new JSONArray();

    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      if (!row.isNull(1)) { // Check if the balance (index 1) is not null
        nonNullBalanceRows.put(row);
      }
    }

    // Create a new JSONObject with the filtered data
    JSONObject filteredResult = new JSONObject();
    filteredResult.put("schema", result.getJSONArray("schema"));
    filteredResult.put("total", nonNullBalanceRows.length()); // Update total count
    filteredResult.put("datarows", nonNullBalanceRows);
    filteredResult.put("size", nonNullBalanceRows.length()); // Update size count

    verifyOrder(
        filteredResult,
        rows("Dale", 4180),
        rows("Nanette", 32838),
        rows("Amber JOHnny", 39225),
        rows("Dillard", 48086));
  }

  private void assertSetEquals(List<Object[]> expected, List<Object[]> actual) {
    Set<List<Object>> expectedSet = new HashSet<>();
    for (Object[] arr : expected) {
      expectedSet.add(Arrays.asList(arr));
    }

    Set<List<Object>> actualSet = new HashSet<>();
    for (Object[] arr : actual) {
      actualSet.add(Arrays.asList(arr));
    }

    assertEquals(expectedSet, actualSet);
  }

  @Test
  public void testSortStringField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | sort lastname | fields lastname", TEST_INDEX_BANK));
    verifyOrder(
        result,
        rows("Adams"),
        rows("Ayala"),
        rows("Bates"),
        rows("Bond"),
        rows("Duke Willmington"),
        rows("Mcpherson"),
        rows("Ratliff"));
  }

  @Test
  public void testSortMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | sort dog_name, age | fields dog_name, age", TEST_INDEX_DOG));
    verifyOrder(result, rows("rex", 2), rows("snoopy", 4));
  }
}
