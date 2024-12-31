/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class ParseCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
  }

  @Test
  public void testParseCommand() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<host>.+)' | fields email, host", TEST_INDEX_BANK));

    // Create the expected rows
    List<Object[]> expectedRows =
        new ArrayList<>(
            List.of(
                new Object[] {"amberduke@pyrami.com", "pyrami.com"},
                new Object[] {"hattiebond@netagy.com", "netagy.com"},
                new Object[] {"nanettebates@quility.com", "quility.com"},
                new Object[] {"daleadams@boink.com", "boink.com"},
                new Object[] {"elinorratliff@scentric.com", "scentric.com"},
                new Object[] {"virginiaayala@filodyne.com", "filodyne.com"},
                new Object[] {"dillardmcpherson@quailcom.com", "quailcom.com"}));

    List<Object[]> actualRows = convertJsonToRows(result, 2);
    sortRowsByFirstColumn(expectedRows);
    sortRowsByFirstColumn(actualRows);
    compareRows(expectedRows, actualRows);
  }

  @Test
  public void testParseCommandReplaceOriginalField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<email>.+)' | fields email", TEST_INDEX_BANK));

    // Create the expected rows
    List<Object[]> expectedRows =
        new ArrayList<>(
            List.of(
                new Object[] {"pyrami.com"},
                new Object[] {"netagy.com"},
                new Object[] {"quility.com"},
                new Object[] {"boink.com"},
                new Object[] {"scentric.com"},
                new Object[] {"filodyne.com"},
                new Object[] {"quailcom.com"}));

    List<Object[]> actualRows = convertJsonToRows(result, 1);
    sortRowsByFirstColumn(expectedRows);
    sortRowsByFirstColumn(actualRows);
    compareRows(expectedRows, actualRows);
  }

  @Test
  public void testParseCommandWithOtherRunTimeFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<host>.+)' | "
                    + "eval eval_result=1 | fields host, eval_result",
                TEST_INDEX_BANK));

    // Create the expected rows as List<Object[]>
    List<Object[]> expectedRows =
        new ArrayList<>(
            List.of(
                new Object[] {"pyrami.com", 1},
                new Object[] {"netagy.com", 1},
                new Object[] {"quility.com", 1},
                new Object[] {"boink.com", 1},
                new Object[] {"scentric.com", 1},
                new Object[] {"filodyne.com", 1},
                new Object[] {"quailcom.com", 1}));

    List<Object[]> actualRows = convertJsonToRows(result, 2);
    sortRowsByFirstColumn(expectedRows);
    sortRowsByFirstColumn(actualRows);
    compareRows(expectedRows, actualRows);
  }

  // Convert JSON response to List<Object[]>
  private List<Object[]> convertJsonToRows(JSONObject result, int columnCount) {
    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      Object[] rowData = new Object[columnCount];
      for (int j = 0; j < columnCount; j++) {
        rowData[j] = row.get(j);
      }
      rows.add(rowData);
    }
    return rows;
  }

  // Sort rows by the first column
  private void sortRowsByFirstColumn(List<Object[]> rows) {
    rows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));
  }

  private void compareRows(List<Object[]> expectedRows, List<Object[]> actualRows) {
    if (expectedRows.size() != actualRows.size()) {
      Assert.fail(
          "Row count is different. expectedRows:" + expectedRows + ", actualRows: " + actualRows);
    }
    for (int i = 0; i < expectedRows.size(); i++) {
      assertArrayEquals(expectedRows.get(i), actualRows.get(i));
    }
  }
}
