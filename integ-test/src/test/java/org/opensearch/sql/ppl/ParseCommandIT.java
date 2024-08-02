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

    // Actual rows from the response
    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> actualRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      actualRows.add(new Object[] {row.getString(0), row.getString(1)});
    }

    if (expectedRows.size() != actualRows.size()) {
      Assert.fail("Row count is different " + expectedRows + " " + actualRows);
    }
    // Sort the lists before compare
    expectedRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));
    actualRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));

    // Compare the sorted lists by iterating over elements and using assertArrayEquals
    for (int i = 0; i < expectedRows.size(); i++) {
      assertArrayEquals(expectedRows.get(i), actualRows.get(i));
    }
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

    // Actual rows from the response
    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> actualRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      actualRows.add(new Object[] {row.getString(0)});
    }

    // Sort the lists using natural ordering
    expectedRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));
    actualRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));

    if (expectedRows.size() != actualRows.size()) {
      Assert.fail("Row count is different " + expectedRows + " " + actualRows);
    }

    // Compare the sorted lists by iterating over elements and using assertArrayEquals
    for (int i = 0; i < expectedRows.size(); i++) {
      assertArrayEquals(expectedRows.get(i), actualRows.get(i));
    }
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

    // Actual rows from the response
    JSONArray dataRows = result.getJSONArray("datarows");
    List<Object[]> actualRows = new ArrayList<>();
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      actualRows.add(new Object[] {row.getString(0), row.getInt(1)});
    }

    if (expectedRows.size() != actualRows.size()) {
      Assert.fail("Row count is different " + expectedRows + " " + actualRows);
    }

    // Sort both expected and actual rows
    expectedRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));
    actualRows.sort((a, b) -> ((String) a[0]).compareTo((String) b[0]));

    // Compare the sorted lists by iterating over elements and using assertArrayEquals
    for (int i = 0; i < expectedRows.size(); i++) {
      assertArrayEquals(expectedRows.get(i), actualRows.get(i));
    }
  }
}
