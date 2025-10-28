/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
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
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DOG);
    loadIndex(Index.WEBLOG);
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
  public void testSortIpField() throws IOException {
    final JSONObject result =
        executeQuery(String.format("source=%s | fields host | sort host", TEST_INDEX_WEBLOGS));
    verifyOrder(
        result,
        rows("::1"),
        rows("::3"),
        rows("::ffff:1234"),
        rows("0.0.0.2"),
        rows("1.2.3.4"),
        rows("1.2.3.5"));
  }

  @Test
  public void testSortMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | sort dog_name, age | fields dog_name, age", TEST_INDEX_DOG));
    verifyOrder(result, rows("rex", 2), rows("snoopy", 4));
  }

  @Test
  public void testSortThenHead() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | sort age | head 2 | fields age", TEST_INDEX_BANK));
    verifyOrder(result, rows(28), rows(32));
  }

  @Test
  public void testSortWithCountLimit() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort 3 - account_number | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(32), rows(25), rows(20));
  }

  @Test
  public void testSortWithCountZero() throws IOException {
    // count=0 should return all results
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort 0 account_number | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testSortWithDesc() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number desc | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testSortWithDescMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort 4 age, - account_number desc | fields age, account_number",
                TEST_INDEX_BANK));
    verifyOrder(result, rows(39, 25), rows(36, 6), rows(36, 20), rows(34, 32));
  }

  @Test
  public void testSortWithStrCast() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort str(account_number) | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(1), rows(13), rows(18), rows(20), rows(25), rows(32), rows(6));
  }

  @Test
  public void testSortWithNumCast() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | sort num(bytes) | fields bytes", TEST_INDEX_WEBLOGS));
    verifyOrder(
        result, rows("1234"), rows("3985"), rows("4085"), rows("4321"), rows("6245"), rows("9876"));
  }

  @Test
  public void testSortWithAsc() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number asc | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testSortWithA() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number a | fields account_number", TEST_INDEX_BANK));
    verifyOrder(result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testSortWithAscMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort age, account_number asc | fields age, account_number",
                TEST_INDEX_BANK));
    verifyOrder(
        result,
        rows(28, 13),
        rows(32, 1),
        rows(33, 18),
        rows(34, 32),
        rows(36, 6),
        rows(36, 20),
        rows(39, 25));
  }

  @Test
  public void testHeadThenSort() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | head 2 | sort age | fields age", TEST_INDEX_BANK));
    if (isPushdownDisabled()) {
      // Pushdown is disabled, it will retrieve the first 2 docs since there's only 1 shard.
      verifyOrder(result, rows(32), rows(36));
    } else {
      verifyOrder(result, rows(28), rows(32));
    }
  }
}
