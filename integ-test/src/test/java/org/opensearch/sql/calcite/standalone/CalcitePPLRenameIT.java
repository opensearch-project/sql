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
            String.format(
                """
                   source = %s | rename age as renamed_age
                   """,
                TEST_INDEX_STATE_COUNTRY));
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
                    String.format(
                        """
                   source = %s | rename age as renamed_age | fields age
                   """,
                        TEST_INDEX_STATE_COUNTRY)));
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
                    String.format(
                        """
                   source = %s | rename renamed_age as age
                   """,
                        TEST_INDEX_STATE_COUNTRY)));
    assertEquals(
        "field [renamed_age] not found; input fields are: [name, country, state, month, year, age,"
            + " _id, _index, _score, _maxscore, _sort, _routing]",
        e.getMessage());
  }

  @Test
  public void testRenameMetaField() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        """
                   source = %s | rename _id as id
                   """,
                        TEST_INDEX_STATE_COUNTRY)));
    assertEquals("Cannot use metadata field [_id] in Rename command.", e.getMessage());

    e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        """
                   source = %s | rename name as _id
                   """,
                        TEST_INDEX_STATE_COUNTRY)));
    assertEquals("Cannot use metadata field [_id] in Rename command.", e.getMessage());
  }

  @Test
  public void testMultipleRename() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | rename name as renamed_name, country as renamed_country
                   | fields renamed_name, age, renamed_country
                   """,
                TEST_INDEX_STATE_COUNTRY));
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
            String.format(
                """
                   source = %s | rename age as user_age | stats avg(user_age) by country
                   """,
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(user_age)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }

  @Test
  public void testMultipleRenameWithBackticks() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s |  rename name as `renamed_name`, country as `renamed_country`
                   | fields `renamed_name`, `age`, `renamed_country`
                   """,
                TEST_INDEX_STATE_COUNTRY));
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
            String.format(
                """
                   source = %s | rename age as `user_age` | stats avg(`user_age`) by country
                   """,
                TEST_INDEX_STATE_COUNTRY));
    verifySchemaInOrder(result, schema("avg(`user_age`)", "double"), schema("country", "string"));
    verifyDataRows(result, rows(22.5, "Canada"), rows(50.0, "USA"));
  }
}
