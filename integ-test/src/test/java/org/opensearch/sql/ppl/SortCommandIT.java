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

    // Filter null balance rows
    List<Object[]> nullRows = filterRows(dataRows, 1, true);

    // Verify the set values for null balances as rows with null balance can return in any order
    List<Object[]> expectedNullRows =
        Arrays.asList(
            new Object[] {"Hattie", null},
            new Object[] {"Elinor", null},
            new Object[] {"Virginia", null});
    assertSetEquals(expectedNullRows, nullRows);

    // Filter non-null balance rows and create filtered result
    List<Object[]> nonNullRows = filterRows(dataRows, 1, false);
    JSONObject filteredResult = createFilteredResult(result, nonNullRows);

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

  // Filter rows by null or non-null values based on the specified column index
  private List<Object[]> filterRows(JSONArray dataRows, int columnIndex, boolean isNull) {
    List<Object[]> filteredRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      if ((isNull && row.isNull(columnIndex)) || (!isNull && !row.isNull(columnIndex))) {
        Object[] rowData = new Object[row.length()];
        for (int j = 0; j < row.length(); j++) {
          rowData[j] = row.isNull(j) ? null : row.get(j);
        }
        filteredRows.add(rowData);
      }
    }
    return filteredRows;
  }

  // Create a new JSONObject with filtered rows and updated metadata
  private JSONObject createFilteredResult(JSONObject originalResult, List<Object[]> filteredRows) {
    JSONArray jsonArray = new JSONArray();
    for (Object[] row : filteredRows) {
      jsonArray.put(new JSONArray(row));
    }

    JSONObject filteredResult = new JSONObject();
    filteredResult.put("schema", originalResult.getJSONArray("schema"));
    filteredResult.put("total", jsonArray.length());
    filteredResult.put("datarows", jsonArray);
    filteredResult.put("size", jsonArray.length());
    return filteredResult;
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
