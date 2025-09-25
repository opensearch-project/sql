/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMultisearchCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.TIME_TEST_DATA2);
  }

  @Test
  public void testBasicMultisearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where age < 30 | eval age_group = \\\"young\\\"] "
                    + "[search source=%s | where age >= 30 | eval age_group = \\\"adult\\\"] "
                    + "| stats count by age_group | sort age_group",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("age_group", null, "string"));
    verifyDataRows(result, rows(549L, "adult"), rows(451L, "young"));
  }

  @Test
  public void testMultisearchSuccessRatePattern() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where balance > 20000 | eval query_type = \\\"good\\\"] "
                    + "[search source=%s | where balance > 0 | eval query_type = \\\"valid\\\"] "
                    + "| stats count(eval(query_type = \\\"good\\\")) as good_accounts, "
                    + "       count(eval(query_type = \\\"valid\\\")) as total_valid",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result, schema("good_accounts", null, "bigint"), schema("total_valid", null, "bigint"));

    verifyDataRows(result, rows(619L, 1000L));
  }

  @Test
  public void testMultisearchWithThreeSubsearches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where state = \\\"IL\\\" | eval region"
                    + " = \\\"Illinois\\\"] [search source=%s | where state = \\\"TN\\\" | eval"
                    + " region = \\\"Tennessee\\\"] [search source=%s | where state = \\\"CA\\\" |"
                    + " eval region = \\\"California\\\"] | stats count by region | sort region",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("region", null, "string"));

    verifyDataRows(result, rows(17L, "California"), rows(22L, "Illinois"), rows(25L, "Tennessee"));
  }

  @Test
  public void testMultisearchWithComplexAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where gender = \\\"M\\\" | eval"
                    + " segment = \\\"male\\\"] [search source=%s | where gender = \\\"F\\\" | eval"
                    + " segment = \\\"female\\\"] | stats count as customer_count, avg(balance) as"
                    + " avg_balance by segment | sort segment",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("customer_count", null, "bigint"),
        schema("avg_balance", null, "double"),
        schema("segment", null, "string"));

    verifyDataRows(
        result, rows(493L, 25623.34685598377, "female"), rows(507L, 25803.800788954635, "male"));
  }

  @Test
  public void testMultisearchWithEmptySubsearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where age > 25] "
                    + "[search source=%s | where age > 200 | eval impossible = \\\"yes\\\"] "
                    + "| stats count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"));

    verifyDataRows(result, rows(733L));
  }

  @Test
  public void testMultisearchWithFieldsProjection() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where gender = \\\"M\\\" | fields"
                    + " firstname, lastname, balance] [search source=%s | where gender = \\\"F\\\""
                    + " | fields firstname, lastname, balance] | head 5",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("firstname", null, "string"),
        schema("lastname", null, "string"),
        schema("balance", null, "bigint"));

    verifyDataRows(
        result,
        rows("Amber", "Duke", 39225L),
        rows("Hattie", "Bond", 5686L),
        rows("Dale", "Adams", 4180L),
        rows("Elinor", "Ratliff", 16418L),
        rows("Mcgee", "Mooney", 18612L));
  }

  @Test
  public void testMultisearchWithTimestampInterleaving() throws IOException {
    // Test multisearch with real timestamp data to verify chronological ordering
    // Use simple approach without eval to focus on timestamp interleaving
    JSONObject result =
        executeQuery(
            "| multisearch [search"
                + " source=opensearch-sql_test_index_time_data | where category IN (\\\"A\\\","
                + " \\\"B\\\")] [search source=opensearch-sql_test_index_time_data2 | where"
                + " category IN (\\\"E\\\", \\\"F\\\")] | head 10");

    // Verify schema - should have 4 fields (timestamp, value, category, @timestamp)
    verifySchema(
        result,
        schema("@timestamp", null, "string"),
        schema("category", null, "string"),
        schema("value", null, "int"),
        schema("timestamp", null, "string"));

    // Test timestamp interleaving: expect results from both indices sorted by timestamp DESC
    // Perfect interleaving demonstrated: E,F from time_test_data2 mixed with A,B from
    // time_test_data
    verifyDataRows(
        result,
        rows("2025-08-01 04:00:00", "E", 2001, "2025-08-01 04:00:00"),
        rows("2025-08-01 03:47:41", "A", 8762, "2025-08-01 03:47:41"),
        rows("2025-08-01 02:30:00", "F", 2002, "2025-08-01 02:30:00"),
        rows("2025-08-01 01:14:11", "B", 9015, "2025-08-01 01:14:11"),
        rows("2025-08-01 01:00:00", "E", 2003, "2025-08-01 01:00:00"),
        rows("2025-07-31 23:40:33", "A", 8676, "2025-07-31 23:40:33"),
        rows("2025-07-31 22:15:00", "F", 2004, "2025-07-31 22:15:00"),
        rows("2025-07-31 21:07:03", "B", 8490, "2025-07-31 21:07:03"),
        rows("2025-07-31 20:45:00", "E", 2005, "2025-07-31 20:45:00"),
        rows("2025-07-31 19:33:25", "A", 9231, "2025-07-31 19:33:25"));
  }

  @Test
  public void testMultisearchWithNonStreamingCommands() throws IOException {
    // Test that previously restricted commands (stats, sort) now work in subsearches
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where age < 30 | stats count() as young_count] "
                    + "[search source=%s | where age >= 30 | stats count() as adult_count] "
                    + "| stats sum(young_count) as total_young, sum(adult_count) as total_adult",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result, schema("total_young", null, "bigint"), schema("total_adult", null, "bigint"));

    verifyDataRows(result, rows(451L, 549L));
  }

  // ========================================================================
  // Type Compatibility Tests
  // ========================================================================

  @Test
  public void testMultisearchIntegerDoubleIncompatible() throws IOException {
    // Test INTEGER + DOUBLE - should fail due to type incompatibility
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch "
                            + "[search source=%s | where age < 30 | eval score = 85] "
                            + "[search source=%s | where age >= 30 | eval score = 95.5] "
                            + "| stats max(score) as max_score",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        exception
            .getMessage()
            .contains("class java.lang.Integer cannot be cast to class java.math.BigDecimal"));
  }

  @Test
  public void testMultisearchIntegerBigintIncompatible() throws IOException {
    // Test INTEGER + BIGINT - should fail due to type incompatibility
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch [search source=%s | where age < 30 | eval id ="
                            + " 100] [search source=%s | where age >= 30 | eval id ="
                            + " 9223372036854775807] | stats max(id) as max_id",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        exception
            .getMessage()
            .contains("class java.lang.Integer cannot be cast to class java.lang.Long"));
  }

  @Test
  public void testMultisearchMultipleIncompatibleTypes() throws IOException {
    // Test multiple incompatible numeric types in one query - should fail
    ResponseException exception =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch [search source=%s | where age < 25 | eval value ="
                            + " 100] [search source=%s | where age >= 25 AND age < 35 | eval value"
                            + " = 9223372036854775807] [search source=%s | where age >= 35 | eval"
                            + " value = 99.99] | stats max(value) as max_value",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        exception
            .getMessage()
            .contains("class java.lang.Integer cannot be cast to class java.math.BigDecimal"));
  }

  @Test
  public void testMultisearchIncompatibleTypes() {
    // Test STRING + NUMERIC conflict - should fail
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch [search source=%s | where age < 30 | eval"
                            + " mixed_field = \\\"text\\\"] [search source=%s | where age >= 30 |"
                            + " eval mixed_field = 123.5] | stats count",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    // Should contain error about incompatible types
    assertTrue(
        "Error message should indicate type incompatibility",
        exception
            .getMessage()
            .contains("Cannot compute compatible row type for arguments to set op"));
  }

  @Test
  public void testMultisearchBooleanIntegerIncompatible() {
    // Test BOOLEAN + INTEGER conflict - should fail
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch "
                            + "[search source=%s | where age < 30 | eval flag = true] "
                            + "[search source=%s | where age >= 30 | eval flag = 42] "
                            + "| stats count",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        "Error message should indicate type incompatibility",
        exception
            .getMessage()
            .contains("Cannot compute compatible row type for arguments to set op"));
  }

  @Test
  public void testMultisearchBooleanStringIncompatible() {
    // Test BOOLEAN + STRING conflict - should fail
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch "
                            + "[search source=%s | where age < 30 | eval status = true] "
                            + "[search source=%s | where age >= 30 | eval status = \\\"active\\\"] "
                            + "| stats count",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        "Error message should indicate type incompatibility",
        exception
            .getMessage()
            .contains("Cannot compute compatible row type for arguments to set op"));
  }

  @Test
  public void testMultisearchWithSingleSubsearchThrowsError() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch " + "[search source=%s | where age > 30]",
                        TEST_INDEX_ACCOUNT)));

    // Should throw a parse error since grammar now enforces at least two subsearches
    assertTrue(
        "Error message should indicate syntax error",
        exception.getMessage().contains("SyntaxCheckException")
            || exception.getMessage().contains("Expecting")
            || exception.getMessage().contains("At least two searches must be specified"));
  }

  // ========================================================================
  // Schema Merge Tests with Different Indices
  // ========================================================================

  @Test
  public void testMultisearchWithDifferentIndicesSchemaMerge() throws IOException {
    // Test schema merging with different indices having different fields
    // ACCOUNT has: firstname, lastname, age, gender, state, employer, email
    // BANK has: sex (instead of gender), age, city (instead of state)
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where age > 35 | fields account_number,"
                    + " firstname, age, balance] [search source=%s | where age > 35 | fields"
                    + " account_number, balance, age] | stats count() as total_count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(result, schema("total_count", null, "bigint"));
    // Verify we get data from both indices by checking we have more than just one index's worth
    verifyDataRows(result, rows(241L)); // Total from both indices combined
  }

  @Test
  public void testMultisearchWithMixedIndicesComplexSchemaMerge() throws IOException {
    // Combine ACCOUNT (banking data) with TIME_TEST_DATA (time series data)
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where balance > 40000 | eval record_type ="
                    + " \\\"financial\\\" | fields account_number, balance, record_type] [search"
                    + " source=%s | where value > 5000 | eval record_type = \\\"timeseries\\\" |"
                    + " fields value, category, record_type] | stats count by record_type | sort"
                    + " record_type",
                TEST_INDEX_ACCOUNT, "opensearch-sql_test_index_time_data"));

    verifySchema(result, schema("count", null, "bigint"), schema("record_type", null, "string"));
    verifyDataRows(result, rows(215L, "financial"), rows(100L, "timeseries"));
  }

  @Test
  public void testMultisearchWithTimeIndicesTimestampOrdering() throws IOException {
    // Test that timestamp ordering works correctly when merging time series data
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where category = \\\"A\\\" | stats count() as count_a] "
                    + "[search source=%s | where category = \\\"E\\\" | stats count() as count_e] "
                    + "| stats sum(count_a) as total_a, sum(count_e) as total_e",
                "opensearch-sql_test_index_time_data", "opensearch-sql_test_index_time_data2"));

    verifySchema(result, schema("total_a", null, "bigint"), schema("total_e", null, "bigint"));

    // Verify we get data from both time series indices
    verifyDataRows(result, rows(26L, 10L)); // Both A and E categories should have data
  }
}
