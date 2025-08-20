/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
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
   * Tests basic field selection functionality including single field selection, multiple fields
   * with comma-delimited syntax, and multiple fields with space-delimited syntax. Validates both
   * schema structure and actual data values returned by each selection method.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testFieldSelection() throws IOException {
    // Test single field selection with schema and data validation
    JSONObject single =
        executeQuery(
            String.format("source=%s | table account_number | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(single, schema("account_number", "bigint"));
    verifyDataRows(single, rows(1), rows(6), rows(13));

    // Test multiple fields with comma-delimited syntax and full data verification
    JSONObject comma =
        executeQuery(
            String.format(
                "source=%s | table account_number, firstname, age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(
        comma,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("age", "bigint"));
    verifyDataRows(comma, rows(1, "Amber", 32), rows(6, "Hattie", 36), rows(13, "Nanette", 28));

    // Test multiple fields with space-delimited syntax produces identical results
    JSONObject space =
        executeQuery(
            String.format(
                "source=%s | table account_number firstname age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(
        space,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("age", "bigint"));
    verifyDataRows(space, rows(1, "Amber", 32), rows(6, "Hattie", 36), rows(13, "Nanette", 28));
  }

  /**
   * Tests various wildcard pattern matching capabilities including prefix wildcards (account*), all
   * fields wildcard (*), middle pattern wildcards (*num*), and multiple wildcard combinations.
   * Validates both schema correctness and data integrity for wildcard field selection patterns.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testWildcardPatterns() throws IOException {
    // Test prefix wildcard pattern matches account_number field
    JSONObject prefix =
        executeQuery(String.format("source=%s | table account* | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(prefix, schema("account_number", "bigint"));
    verifyNumOfRows(prefix, 1);

    // Test all fields wildcard selects all available fields
    JSONObject all =
        executeQuery(String.format("source=%s | table * | head 1", TEST_INDEX_ACCOUNT));

    verifySchema(
        all,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("age", "bigint"),
        schema("gender", "string"),
        schema("address", "string"),
        schema("employer", "string"),
        schema("email", "string"),
        schema("city", "string"),
        schema("state", "string"));
    verifyNumOfRows(all, 1);

    // Test middle pattern wildcard with explicit fields and data verification
    JSONObject middle =
        executeQuery(
            String.format("source=%s | table firstname, *num*, age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(
        middle,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("age", "bigint"));
    verifyDataRows(middle, rows("Amber", 1, 32), rows("Hattie", 6, 36), rows("Nanette", 13, 28));

    // Test multiple wildcard patterns matching different field types
    JSONObject multiple =
        executeQuery(
            String.format("source=%s | table *name, *number, age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(
        multiple,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("account_number", "bigint"),
        schema("age", "bigint"));
    verifyNumOfRows(multiple, 3);
  }

  /**
   * Tests table command integration with other PPL commands including sort, where (filter), and
   * stats operations. Verifies that table command works correctly when combined with data
   * transformation and aggregation operations in various pipeline configurations.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testPipelineIntegration() throws IOException {
    // Test table command after sort operation
    JSONObject sorted =
        executeQuery(
            String.format(
                "source=%s | sort age | table account_number, age | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(sorted, schema("account_number", "bigint"), schema("age", "bigint"));
    verifyNumOfRows(sorted, 3);

    // Test table command after where filter
    JSONObject filtered =
        executeQuery(
            String.format(
                "source=%s | where age > 35 | table account_number, age | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(filtered, schema("account_number", "bigint"), schema("age", "bigint"));
    verifyNumOfRows(filtered, 3);

    // Test table command before stats aggregation
    JSONObject stats =
        executeQuery(
            String.format(
                "source=%s | table account_number, state | stats count() by state | sort state |"
                    + " head 10",
                TEST_INDEX_ACCOUNT));
    verifySchema(stats, schema("count()", "bigint"), schema("state", "string"));
    verifyNumOfRows(stats, 10);
  }

  /**
   * Tests field order preservation in result schema and field deduplication behavior. Validates
   * that field order matches specification, duplicate fields are removed, and wildcard overlaps
   * with explicit fields are handled correctly with full data verification.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testFieldOrderAndDeduplication() throws IOException {
    // Test field order preservation with schema and data validation
    JSONObject order =
        executeQuery(
            String.format(
                "source=%s | table firstname, account_number, age | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(
        order,
        schema("firstname", "string"),
        schema("account_number", "bigint"),
        schema("age", "bigint"));
    verifyDataRows(order, rows("Amber", 1, 32));

    // Test explicit duplicate field removal with schema and data verification
    JSONObject duplicates =
        executeQuery(
            String.format(
                "source=%s | table firstname, firstname, age | head 1", TEST_INDEX_ACCOUNT));
    verifySchema(duplicates, schema("firstname", "string"), schema("age", "bigint"));
    verifyDataRows(duplicates, rows("Amber", 32));

    // Test wildcard and explicit field deduplication with complete data validation
    JSONObject wildcardDedup =
        executeQuery(String.format("source=%s | table age, a* | head 3", TEST_INDEX_ACCOUNT));
    verifySchema(
        wildcardDedup,
        schema("age", "bigint"),
        schema("account_number", "bigint"),
        schema("address", "string"));
    verifyDataRows(
        wildcardDedup,
        rows(32, 1, "880 Holmes Lane"),
        rows(36, 6, "671 Bristol Street"),
        rows(28, 13, "789 Madison Street"));
  }

  /**
   * Tests complex multi-command pipelines demonstrating table command as the final presentation
   * layer, integration with fields command for pre-filtering, and compatibility with advanced
   * aggregation pipelines. Validates the recommended practice of using table for final field
   * selection.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testComplexPipelines() throws IOException {
    // Test table as final command in multi-operation pipeline
    JSONObject lastCommand =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | sort - age | eval ratio = age/10 | table firstname,"
                    + " age, ratio",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        lastCommand,
        schema("firstname", "string"),
        schema("age", "bigint"),
        schema("ratio", "bigint"));
    verifyNumOfRows(lastCommand, 502);

    // Test table with fields command for pre-filtering
    JSONObject withFields =
        executeQuery(
            String.format(
                "source=%s | fields firstname, lastname, age, state | where age > 35 | table"
                    + " firstname, lastname, state",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        withFields,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("state", "string"));
    verifyNumOfRows(withFields, 238);

    // Test table in complex aggregation pipeline
    JSONObject statsComplex =
        executeQuery(
            String.format(
                "source=%s | stats count() as cnt, avg(age) as avgAge by state | sort - avgAge |"
                    + " table state, avgAge, cnt",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        statsComplex,
        schema("state", "string"),
        schema("avgAge", "double"),
        schema("cnt", "bigint"));
    verifyNumOfRows(statsComplex, 51);
  }

  /**
   * Tests table command positioning within pipelines, including table command in the middle of a
   * pipeline followed by other operations, and multiple table commands for progressive field
   * refinement. Verifies that table commands work correctly at any pipeline position.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTablePositioning() throws IOException {
    // Test table command in middle of pipeline with subsequent operations
    JSONObject middle =
        executeQuery(
            String.format(
                "source=%s | table firstname, age, state | where age > 35 | sort - age",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        middle, schema("firstname", "string"), schema("age", "bigint"), schema("state", "string"));
    verifyNumOfRows(middle, 238);

    // Test multiple table commands for progressive field selection
    JSONObject multiple =
        executeQuery(
            String.format(
                "source=%s | table firstname, lastname, age, state | where age > 35 | table"
                    + " firstname, age",
                TEST_INDEX_ACCOUNT));
    verifySchema(multiple, schema("firstname", "string"), schema("age", "bigint"));
    verifyNumOfRows(multiple, 238);
  }

  /**
   * Tests complex query scenarios with filtering, descending sort operations, and field selection
   * using both comma-delimited and space-delimited syntax. Validates that both syntax styles
   * produce identical results in complex pipeline operations.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testComplexQueries() throws IOException {
    // Test complex query with comma-delimited field syntax
    JSONObject comma =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | sort - balance | table account_number,"
                    + " firstname, balance | head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        comma,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));
    verifyNumOfRows(comma, 2);

    // Test complex query with space-delimited field syntax
    JSONObject space =
        executeQuery(
            String.format(
                "source=%s | where balance > 30000 | sort - balance | table account_number"
                    + " firstname balance | head 2",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        space,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("balance", "bigint"));
    verifyNumOfRows(space, 2);
  }

  /**
   * Tests table command integration with advanced PPL operations including specific value
   * filtering, field renaming, deduplication, and field evaluation with conditional logic. Verifies
   * that table command correctly handles renamed fields and computed fields from eval operations.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testAdvancedOperations() throws IOException {
    // Test table with specific state value filtering
    JSONObject filtered =
        executeQuery(
            String.format(
                "source=%s | where state='CA' | table account_number, state, firstname, age | head"
                    + " 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        filtered,
        schema("account_number", "bigint"),
        schema("state", "string"),
        schema("firstname", "string"),
        schema("age", "bigint"));
    verifyNumOfRows(filtered, 3);

    // Test table with renamed fields
    JSONObject renamed =
        executeQuery(
            String.format(
                "source=%s | rename firstname as first_name, lastname as last_name | table"
                    + " account_number, first_name, last_name | head 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        renamed,
        schema("account_number", "bigint"),
        schema("first_name", "string"),
        schema("last_name", "string"));
    verifyNumOfRows(renamed, 3);

    // Test table with deduplication and computed fields from eval
    JSONObject dedupEval =
        executeQuery(
            String.format(
                "source=%s | dedup state | eval region=case(state='CA', 'west' else 'other') |"
                    + " table state, region | sort state",
                TEST_INDEX_ACCOUNT));
    verifySchema(dedupEval, schema("state", "string"), schema("region", "string"));
    verifyNumOfRows(dedupEval, 51);
  }
}
