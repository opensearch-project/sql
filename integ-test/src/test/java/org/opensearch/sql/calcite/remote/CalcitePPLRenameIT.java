/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLRenameIT extends PPLIntegTestCase {

  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source = %s | rename age as renamed_age", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("renamed_age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testRefRenamedField() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | rename age as renamed_age | fields age",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(
        e,
        "field [age] not found; input fields are: [name, country, state, month, year, renamed_age,"
            + " _id, _index, _score, _maxscore, _sort, _routing]");
  }

  @Test
  public void testRenameNotExistedField() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | rename renamed_age as age", TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(
        e,
        "field [renamed_age] not found; input fields are: [name, country, state, month, year, age,"
            + " _id, _index, _score, _maxscore, _sort, _routing]");
  }

  @Test
  public void testRenameToMetaField() throws IOException {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format("source = %s | rename name as _id", TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Cannot use metadata field [_id] in Rename command.");

    // Test rename to _ID, which is allowed as metadata fields name is case-sensitive
    JSONObject result =
        executeQuery(String.format("source = %s | rename age as _ID", TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("_ID", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
  }

  @Test
  public void testMultipleRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename name as renamed_name, country as renamed_country"
                    + "| fields renamed_name, age, renamed_country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "int"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameInAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename age as user_age | stats avg(user_age) by country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(user_age)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }

  @Test
  public void testMultipleRenameWithBackticks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s |  rename name as `renamed_name`, country as `renamed_country`"
                    + "| fields `renamed_name`, `age`, `renamed_country`",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "int"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameWithBackticksInAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | rename age as `user_age` | stats avg(`user_age`) by country",
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(`user_age`)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }
}
