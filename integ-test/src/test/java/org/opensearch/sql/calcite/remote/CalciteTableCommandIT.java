/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for the Calcite table command in PPL queries. These tests verify the
 * functionality of the 'table' command with various combinations of other PPL commands like sort,
 * filter, stats, etc.
 *
 * <p>The tests use the account index data and verify both schema and data results.
 */
public class CalciteTableCommandIT extends PPLIntegTestCase {

  /**
   * Initialize test environment before running tests. Sets up the test environment by enabling
   * Calcite engine, disabling fallback to legacy engine, and loading the account index.
   *
   * @throws Exception if initialization fails
   */
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.ACCOUNT);
  }

  /**
   * Tests the basic table command with a single field. Verifies that the table command correctly
   * selects a single field and returns the expected schema and data rows.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testBasicTable() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table account_number | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("account_number", "bigint"));

    verifyDataRows(actual, rows(1), rows(6), rows(13));
  }

  /**
   * Tests the table command with multiple fields. Verifies that the table command correctly selects
   * multiple fields and returns the expected schema and data rows with all specified fields.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithMultipleFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table account_number, firstname, age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("age", "bigint"));

    verifyDataRows(actual, rows(1, "Amber", 32), rows(6, "Hattie", 36), rows(13, "Nanette", 28));
  }

  /**
   * Tests the table command with wildcard field selection. Verifies that the table command
   * correctly selects all fields that match the 'account*' pattern.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithAllFields() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table account* | head 1", TEST_INDEX_ACCOUNT));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should return exactly one row", datarows.length() == 1);
    assertTrue(
        "Should have at least account_number field", actual.getJSONArray("schema").length() >= 1);
  }

  /**
   * Tests the table command with sort operation. Verifies that the table command correctly works
   * with the sort command to return data in the specified order (ascending by age).
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithSort() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort age | table account_number, age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("account_number", "bigint"), schema("age", "bigint"));

    JSONArray datarows = actual.getJSONArray("datarows");
    long prevAge = 0;
    for (int i = 0; i < datarows.length(); i++) {
      long currentAge = datarows.getJSONArray(i).getLong(1);
      assertTrue("Ages should be in ascending order", currentAge >= prevAge);
      prevAge = currentAge;
    }
  }

  /**
   * Tests the table command with filter operation. Verifies that the table command correctly works
   * with the where command to filter data before selecting fields.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age > 35 | table account_number, age | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("account_number", "bigint"), schema("age", "bigint"));

    JSONArray datarows = actual.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      long age = datarows.getJSONArray(i).getLong(1);
      assertTrue("All ages should be greater than 35", age > 35);
    }
  }

  /**
   * Tests the table command with stats operation. Verifies that the table command correctly works
   * with the stats command to aggregate data by state.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithStats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table account_number, state | stats count() by state | sort state",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("count()", "bigint"), schema("state", "string"));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should have multiple states in the result", datarows.length() > 10);
  }

  /**
   * Tests that the table command preserves field order. Verifies that fields appear in the result
   * schema in the same order as specified in the table command.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableFieldOrder() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, account_number, age | head 1", TEST_INDEX_ACCOUNT));

    JSONArray schema = actual.getJSONArray("schema");
    assertEquals(
        "First field should be firstname", "firstname", schema.getJSONObject(0).getString("name"));
    assertEquals(
        "Second field should be account_number",
        "account_number",
        schema.getJSONObject(1).getString("name"));
    assertEquals("Third field should be age", "age", schema.getJSONObject(2).getString("name"));
  }

  /**
   * Tests the table command with duplicate field specifications. Verifies that the table command
   * handles duplicate field names gracefully.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithDuplicateFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table account_number, account_number | head 1", TEST_INDEX_ACCOUNT));

    JSONArray schema = actual.getJSONArray("schema");
    assertTrue("Schema should contain at least one field", schema.length() >= 1);
  }

  /**
   * Tests error handling when a non-existent field is specified. Verifies that the table command
   * throws an appropriate exception when a field that doesn't exist is requested.
   */
  @Test
  public void testTableFieldNotFound() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalStateException.class,
            () ->
                executeQuery(
                    String.format("source=%s | table nonexistent_field", TEST_INDEX_ACCOUNT)));

    verifyErrorMessageContains(e, "field [nonexistent_field] not found");
  }

  /**
   * Tests the table command in a complex query with multiple operations. Verifies that the table
   * command works correctly in a pipeline with filtering, sorting in descending order, and field
   * selection.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithComplexQuery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | sort - balance | table account_number,"
                    + " firstname, balance | head 2",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));

    JSONArray datarows = actual.getJSONArray("datarows");
    long prevBalance = Long.MAX_VALUE;
    for (int i = 0; i < datarows.length(); i++) {
      long balance = datarows.getJSONArray(i).getLong(2);
      assertTrue("All balances should be greater than 30000", balance > 30000);
      assertTrue("Balances should be in descending order", balance <= prevBalance);
      prevBalance = balance;
    }
  }

  /**
   * Tests the table command with a specific state filter. Verifies that the table command works
   * correctly with a filter that selects only records from California (CA).
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithWildcardFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where state='CA' | table account_number, state, firstname, age | head"
                    + " 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("state", "string"),
        schema("firstname", "string"),
        schema("age", "bigint"));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should return at least one record from CA", datarows.length() > 0);
    for (int i = 0; i < datarows.length(); i++) {
      String state = datarows.getJSONArray(i).getString(1);
      assertEquals("All records should be from California", "CA", state);
    }
  }

  /**
   * Tests the table command with field renaming. Verifies that the table command works correctly
   * with renamed fields after applying the rename command.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithRenameAndWildcard() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | rename firstname as first_name, lastname as last_name | table"
                    + " account_number, first_name, last_name | head 3",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("first_name", "string"),
        schema("last_name", "string"));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should return at least one row", datarows.length() > 0);

    JSONArray firstRow = datarows.getJSONArray(0);
    assertTrue("Account number should be positive", firstRow.getLong(0) > 0);
  }

  /**
   * Tests the table command with deduplication and field evaluation. Verifies that the table
   * command works correctly after deduplicating states and creating a new calculated field using
   * the eval command.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithDedupAndEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | dedup state | eval region=case(state='CA', 'west' else 'other') |"
                    + " table state, region | sort state",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("state", "string"), schema("region", "string"));

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should return at least one row", datarows.length() > 0);
    for (int i = 0; i < datarows.length(); i++) {
      String region = datarows.getJSONArray(i).getString(1);
      assertTrue(
          "Region should be either 'west' or 'other'",
          region.equals("west") || region.equals("other"));
    }
  }

  /**
   * Tests the table command with wildcard field selection. Verifies that the table command
   * correctly selects all fields that match the wildcard pattern 'account*'.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithWildcardFields() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table account* | head 3", TEST_INDEX_ACCOUNT));

    JSONArray schema = actual.getJSONArray("schema");
    boolean hasAccountField = false;
    for (int i = 0; i < schema.length(); i++) {
      String fieldName = schema.getJSONObject(i).getString("name");
      if (fieldName.startsWith("account")) {
        hasAccountField = true;
        break;
      }
    }
    assertTrue("Schema should contain at least one field starting with 'account'", hasAccountField);

    JSONArray datarows = actual.getJSONArray("datarows");
    assertTrue("Should return at least one row", datarows.length() > 0);
  }
}
