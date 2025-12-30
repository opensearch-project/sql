/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

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
        rows("balance  ", "39225", "5686", "32838", "4180", "16418"),
        rows("age      ", "32", "36", "28", "33", "36"));
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
            "balance  ",
            "39225",
            "5686",
            "32838",
            "4180",
            "16418",
            "40540",
            "48086",
            "18612",
            "34487",
            "29104",
            null,
            null,
            null,
            null),
        rows(
            "age      ",
            "32",
            "36",
            "28",
            "33",
            "36",
            "39",
            "34",
            "39",
            "37",
            "23",
            null,
            null,
            null,
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
        rows("balance  ", "39225", "5686", "32838", "4180", "16418"),
        rows("age      ", "32", "36", "28", "33", "36"));
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
        rows("balance  ", "39225", "5686", "32838", "4180", "16418"),
        rows("age      ", "32", "36", "28", "33", "36"));
  }
}
