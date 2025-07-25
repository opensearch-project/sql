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
   * Tests the table command with multiple fields using comma-delimited syntax. Verifies that the
   * table command correctly selects multiple fields and returns the expected schema and data rows.
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
   * Tests the table command with multiple fields using space-delimited syntax. Verifies that the
   * table command correctly selects multiple fields without commas and returns the expected schema
   * and data.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithSpaceDelimitedFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table account_number firstname age | head 3", TEST_INDEX_ACCOUNT));

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
  public void testTableWithPatternWildcard() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table account* | head 1", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 1);
  }

  /**
   * Tests the table command with star wildcard to select all fields. Verifies that the table
   * command correctly selects all available fields when using the '*' wildcard.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithAllFieldsWildcard() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table * | head 1", TEST_INDEX_ACCOUNT));

    JSONArray schema = actual.getJSONArray("schema");

    verifyNumOfRows(actual, 1);

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("age", "bigint"));
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

    verifyNumOfRows(actual, 3);
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

    verifyNumOfRows(actual, 3);
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

    verifyNumOfRows(actual, 51);
  }

  /**
   * Tests that the table command preserves field order with comma-delimited syntax. Verifies that
   * fields appear in the result schema in the same order as specified in the table command.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableFieldOrder() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, account_number, age | head 1", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 1);
  }

  /**
   * Tests that the table command preserves field order with space-delimited syntax. Verifies that
   * fields appear in the result schema in the same order as specified in the table command without
   * commas.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableSpaceDelimitedFieldOrder() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname account_number age | head 1", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 1);
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

    verifyNumOfRows(actual, 1);
  }

  /**
   * Tests the table command with duplicate field specifications. Verifies that the table command
   * handles duplicate field names gracefully by including them only once in the result.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithDuplicateFieldsExplicit() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, firstname, age | head 1", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 1);

    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));
  }

  /**
   * Tests complex wildcard patterns that match characters in the middle of field names. Verifies
   * that fields containing specific characters are correctly selected.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithWildcardPattern() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname, *num* | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("firstname", "string"), schema("account_number", "bigint"));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests wildcard pattern matching for fields with a specific prefix. Verifies that fields
   * starting with a specific prefix are correctly selected.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithSpecificPrefixWildcard() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname, age, acc* | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("age", "bigint"),
        schema("account_number", "bigint"));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests mixed field specification with regular fields and wildcards. This test specifically
   * focuses on interspersing wildcards between regular fields in a specific order.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithMixedFieldSpecification() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, a*, lastname | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests table command with multiple wildcards matching different patterns. This test verifies
   * that fields matching different wildcard patterns are correctly selected.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithMultipleWildcardTypes() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table *name, *number, age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("account_number", "bigint"),
        schema("age", "bigint"));

    // Schema already verified above
  }

  /**
   * Tests multiple wildcard patterns in field selection. This test specifically focuses on using
   * multiple different wildcard patterns in a single query.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithMultipleWildcardPatterns() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname, f*, a* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests space-delimited only syntax without commas. Verifies that fields can be specified with
   * spaces only, without requiring commas as separators.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableSpaceDelimitedOnly() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname lastname age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests space-delimited syntax with wildcard field selection. Verifies that fields starting with
   * a specific prefix are correctly selected using space-delimited syntax.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableSpaceDelimitedWildcardFieldsStarting() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table acc* | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("account_number", "bigint"));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests space-delimited syntax with multiple wildcard patterns. Verifies that multiple wildcard
   * patterns work correctly with space-delimited syntax.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableSpaceDelimitedMultipleWildcards() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname f* a* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests comma-delimited syntax with wildcard patterns. Verifies that comma-delimited syntax works
   * correctly with wildcard patterns.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableCommaDelimitedWithWildcards() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table firstname,*num*,age | head 3", TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("age", "bigint"));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests the best practice of placing the table command at the end of search pipelines. This
   * demonstrates the recommended pattern for optimal performance.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableAsLastCommand() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | sort - age | eval ratio = age/10 | table firstname,"
                    + " age, ratio",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual, schema("firstname", "string"), schema("age", "bigint"), schema("ratio", "double"));

    verifyNumOfRows(actual, 1000);
  }

  /**
   * Tests using the fields command for filtering operations and table for presentation. This
   * demonstrates the recommended practice of using fields for filtering and table for final
   * display.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testFieldsForFilteringTableForPresentation() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields firstname, lastname, age, state | where age > 35 | table"
                    + " firstname, lastname, state",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("state", "string"));

    // Schema already verified above - age field correctly excluded
  }

  /**
   * Tests selecting a large number of fields with wildcards for performance optimization. This
   * demonstrates how to efficiently select related fields using wildcards.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithLargeFieldSetOptimization() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table a*, *name, s* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests compatibility with other PPL commands in a complex pipeline. This demonstrates how table
   * integrates with the existing PPL command architecture.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableCompatibilityWithOtherCommands() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count() as cnt, avg(age) as avgAge by state | sort - avgAge |"
                    + " table state, avgAge, cnt",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual, schema("state", "string"), schema("avgAge", "double"), schema("cnt", "bigint"));

    verifyNumOfRows(actual, 51);
  }

  /**
   * Tests table command in the middle of a pipeline followed by other commands. This verifies that
   * table can be used to select fields before further processing.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableInMiddleOfPipeline() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, age, state | where age > 35 | sort - age",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual, schema("firstname", "string"), schema("age", "bigint"), schema("state", "string"));

    verifyNumOfRows(actual, 362);
  }

  /**
   * Tests multiple table commands in a pipeline. This verifies that table commands can be used to
   * progressively refine field selection.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testMultipleTableCommands() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table firstname, lastname, age, state | where age > 35 | table"
                    + " firstname, age",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("firstname", "string"), schema("age", "bigint"));

    verifyNumOfRows(actual, 362);

    verifyNumOfRows(actual, 362);
  }

  /**
   * Tests table command with wildcard patterns in the middle of a pipeline. This verifies that
   * wildcard field selection works correctly when not at the end of the pipeline.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithWildcardInMiddleOfPipeline() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | table a*, firstname, s* | where age > 35 | sort state",
                TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 362);
  }

  /**
   * Tests the table command in a complex query with multiple operations. Verifies that the table
   * command works correctly in a pipeline with filtering, sorting in descending order, and field
   * selection using comma-delimited syntax.
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

    verifyNumOfRows(actual, 2);
  }

  /**
   * Tests the table command in a complex query with multiple operations using space-delimited
   * syntax. Verifies that the table command works correctly in a pipeline with filtering, sorting
   * in descending order, and field selection without commas.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithSpaceDelimitedComplexQuery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | sort - balance | table account_number"
                    + " firstname balance | head 2",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));

    verifyNumOfRows(actual, 2);
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

    verifyNumOfRows(actual, 3);
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

    verifyNumOfRows(actual, 3);
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

    verifyNumOfRows(actual, 51);
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

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests the table command with multiple wildcard patterns using space-delimited syntax. Verifies
   * that the table command correctly selects fields matching multiple wildcard patterns.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableWithSpaceDelimitedWildcards() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table account* age* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests field deduplication when multiple wildcard patterns match the same field. Verifies that
   * fields appear only once even when matched by multiple patterns.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableFieldDeduplicationWithWildcards() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table a*, *age | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests field deduplication with explicit field and wildcard pattern. Verifies that explicitly
   * named fields don't duplicate when also matched by wildcards.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableFieldDeduplicationExplicitAndWildcard() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | table age, a* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }

  /**
   * Tests field deduplication with multiple overlapping patterns. Verifies that fields matched by
   * multiple patterns appear only once.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTableFieldDeduplicationMultipleOverlaps() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | table *name, first*, f* | head 3", TEST_INDEX_ACCOUNT));

    verifyNumOfRows(actual, 3);
  }
}
