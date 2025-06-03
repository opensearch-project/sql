/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLRenameIT extends CalcitePPLIntegTestCase {

  public void init() throws IOException {
    super.init();

    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testRename() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_STATE_COUNTRY + " | rename age as renamed_age\n");
    verifySchema(
        result,
        schema("name", "string"),
        schema("renamed_age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "integer"),
        schema("month", "integer"));
  }

  @Test
  public void testRefRenamedField() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                        "source = " + TEST_INDEX_STATE_COUNTRY + " | rename age as renamed_age | fields age\n"));
    assertEquals(
        "field [age] not found; input fields are: [name, country, state, month, year, renamed_age,"
            + " _id, _index, _score, _maxscore, _sort, _routing]",
        e.getMessage());
  }

  @Test
  public void testRenameNotExistedField() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                        "source = " + TEST_INDEX_STATE_COUNTRY + " | rename renamed_age as age\n"));
    assertEquals(
        "field [renamed_age] not found; input fields are: [name, country, state, month, year, age,"
            + " _id, _index, _score, _maxscore, _sort, _routing]",
        e.getMessage());
  }

  @Test
  public void testRenameToMetaField() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                            "source = %s | rename name as _id\n",
                        TEST_INDEX_STATE_COUNTRY)));
    assertEquals("Cannot use metadata field [_id] in Rename command.", e.getMessage());

    // Test rename to _ID, which is allowed as metadata fields name is case-sensitive
    JSONObject result =
        executeQuery(
            String.format(
                    "source = %s | rename age as _ID\n",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("_ID", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "integer"),
        schema("month", "integer"));
  }

  @Test
  public void testMultipleRename() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_STATE_COUNTRY + " | rename name as renamed_name, country as renamed_country\n| fields renamed_name, age, renamed_country\n");
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "integer"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameInAgg() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_STATE_COUNTRY + " | rename age as user_age | stats avg(user_age) by country\n");
    verifySchemaInOrder(result, schema("avg(user_age)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }

  @Test
  public void testMultipleRenameWithBackticks() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_STATE_COUNTRY + " |  rename name as `renamed_name`, country as `renamed_country`\n| fields `renamed_name`, `age`, `renamed_country`\n");
    verifySchema(
        result,
        schema("renamed_name", "string"),
        schema("age", "integer"),
        schema("renamed_country", "string"));
    verifyDataRows(
        result,
        rows("Jake", 70, "USA"),
        rows("Hello", 30, "USA"),
        rows("John", 25, "Canada"),
        rows("Jane", 20, "Canada"));
  }

  @Test
  public void testRenameWithBackticksInAgg() {
    JSONObject result =
        executeQuery(
                "source = " + TEST_INDEX_STATE_COUNTRY + " | rename age as `user_age` | stats avg(`user_age`) by country\n");
    verifySchemaInOrder(result, schema("avg(`user_age`)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }
}
