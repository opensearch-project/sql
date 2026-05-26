/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTransposeCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  /**
   * default test without parameters on account index
   *
   * @throws IOException
   */
  @Test
  public void testTranspose() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s |  head 5 | fields firstname,  age, balance | transpose",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result,
        schema("column", "string"),
        schema("row 1", "string"),
        schema("row 2", "string"),
        schema("row 3", "string"),
        schema("row 4", "string"),
        schema("row 5", "string"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");

    // Iterate through all data rows
    verifyDataRows(
        result,
        rows("firstname", "Amber", "Hattie", "Nanette", "Dale", "Elinor"),
        rows("balance", "39225", "5686", "32838", "4180", "16418"),
        rows("age", "32", "36", "28", "33", "36"));
  }

  @Test
  public void testTransposeLimit() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s |  head 10 | fields firstname ,  age, balance | transpose 14",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result,
        schema("column", "string"),
        schema("row 1", "string"),
        schema("row 2", "string"),
        schema("row 3", "string"),
        schema("row 4", "string"),
        schema("row 5", "string"),
        schema("row 6", "string"),
        schema("row 7", "string"),
        schema("row 8", "string"),
        schema("row 9", "string"),
        schema("row 10", "string"),
        schema("row 11", "string"),
        schema("row 12", "string"),
        schema("row 13", "string"),
        schema("row 14", "string"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    verifyDataRows(
        result,
        rows(
            "firstname",
            "Amber",
            "Hattie",
            "Nanette",
            "Dale",
            "Elinor",
            "Virginia",
            "Dillard",
            "Mcgee",
            "Aurelia",
            "Fulton",
            null,
            null,
            null,
            null),
        rows(
            "balance", "39225", "5686", "32838", "4180", "16418", "40540", "48086", "18612",
            "34487", "29104", null, null, null, null),
        rows(
            "age", "32", "36", "28", "33", "36", "39", "34", "39", "37", "23", null, null, null,
            null));
  }

  @Test
  public void testTransposeLowerLimit() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s |  head 15 | fields firstname ,  age, balance | transpose 5",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result,
        schema("column", "string"),
        schema("row 1", "string"),
        schema("row 2", "string"),
        schema("row 3", "string"),
        schema("row 4", "string"),
        schema("row 5", "string"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");
    // Iterate through all data rows
    verifyDataRows(
        result,
        rows("firstname", "Amber", "Hattie", "Nanette", "Dale", "Elinor"),
        rows("balance", "39225", "5686", "32838", "4180", "16418"),
        rows("age", "32", "36", "28", "33", "36"));
  }

  /**
   * Regression test for #5172: transpose fails when input has a field named 'value', because the
   * internal unpivot column was also hardcoded as 'value'.
   */
  @Test
  public void testTransposeWithValueFieldNameCollision() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | stats count() as value, avg(age) as avg_age | transpose",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("column", "string"),
        schema("row 1", "string"),
        schema("row 2", "string"),
        schema("row 3", "string"),
        schema("row 4", "string"),
        schema("row 5", "string"));

    var dataRows = result.getJSONArray("datarows");
    // Verify that each transposed row has distinct correct values
    // (not all duplicated from the 'value' field)
    assertEquals(2, dataRows.length());
    boolean foundValue = false;
    boolean foundAvgAge = false;
    for (int i = 0; i < dataRows.length(); i++) {
      var row = dataRows.getJSONArray(i);
      String colName = row.getString(0);
      if ("value".equals(colName)) {
        foundValue = true;
        // count should be 1000 (total accounts)
        assertEquals("1000", row.getString(1));
      } else if ("avg_age".equals(colName)) {
        foundAvgAge = true;
        // avg_age should not equal the count value
        assertNotEquals("1000", row.getString(1));
      }
    }
    assertTrue("Should have 'value' row in transposed result", foundValue);
    assertTrue("Should have 'avg_age' row in transposed result", foundAvgAge);
  }

  @Test
  public void testTransposeColumnName() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s |  head 5 | fields firstname,  age, balance | transpose 5"
                    + " column_name='column_names'",
                TEST_INDEX_ACCOUNT));

    // Verify that we get original rows plus totals row
    verifySchema(
        result,
        schema("column_names", "string"),
        schema("row 1", "string"),
        schema("row 2", "string"),
        schema("row 3", "string"),
        schema("row 4", "string"),
        schema("row 5", "string"));

    // Should have original data plus one totals row
    var dataRows = result.getJSONArray("datarows");

    // Iterate through all data rows
    verifyDataRows(
        result,
        rows("firstname", "Amber", "Hattie", "Nanette", "Dale", "Elinor"),
        rows("balance", "39225", "5686", "32838", "4180", "16418"),
        rows("age", "32", "36", "28", "33", "36"));
  }
}
