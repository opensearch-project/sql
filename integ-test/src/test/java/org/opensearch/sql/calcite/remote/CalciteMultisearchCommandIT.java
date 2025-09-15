/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
  }

  @Test
  public void testBasicMultisearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where age < 30 | eval age_group = \\\"young\\\"] "
                    + "[search source=%s | where age >= 30 | eval age_group = \\\"adult\\\"] "
                    + "| stats count by age_group | sort age_group",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("age_group", null, "string"));
    verifyDataRows(result, rows(549L, "adult"), rows(451L, "young"));
  }

  @Test
  public void testMultisearchSuccessRatePattern() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where balance > 20000 | eval query_type = \\\"good\\\"] "
                    + "[search source=%s | where balance > 0 | eval query_type = \\\"valid\\\"] "
                    + "| stats count(eval(query_type = \\\"good\\\")) as good_accounts, "
                    + "       count(eval(query_type = \\\"valid\\\")) as total_valid",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result, schema("good_accounts", null, "bigint"), schema("total_valid", null, "bigint"));

    verifyDataRows(result, rows(619L, 1000L));
  }

  @Test
  public void testMultisearchWithThreeSubsearches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch [search source=%s | where state = \\\"IL\\\" | eval region"
                    + " = \\\"Illinois\\\"] [search source=%s | where state = \\\"TN\\\" | eval"
                    + " region = \\\"Tennessee\\\"] [search source=%s | where state = \\\"CA\\\" |"
                    + " eval region = \\\"California\\\"] | stats count by region | sort region",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("region", null, "string"));

    verifyDataRows(result, rows(17L, "California"), rows(22L, "Illinois"), rows(25L, "Tennessee"));
  }

  @Test
  public void testMultisearchWithComplexAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch [search source=%s | where gender = \\\"M\\\" | eval"
                    + " segment = \\\"male\\\"] [search source=%s | where gender = \\\"F\\\" | eval"
                    + " segment = \\\"female\\\"] | stats count as customer_count, avg(balance) as"
                    + " avg_balance by segment | sort segment",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

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
                "source=%s | multisearch "
                    + "[search source=%s | where age > 25] "
                    + "[search source=%s | where age > 200 | eval impossible = \\\"yes\\\"] "
                    + "| stats count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"));

    verifyDataRows(result, rows(733L));
  }

  @Test
  public void testMultisearchWithFieldsProjection() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch [search source=%s | where gender = \\\"M\\\" | fields"
                    + " firstname, lastname, balance] [search source=%s | where gender = \\\"F\\\""
                    + " | fields firstname, lastname, balance] | head 5",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

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
  public void testMultisearchWithBalanceCategories() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch [search source=%s | where balance > 40000 | eval"
                    + " balance_category = \\\"high\\\"] [search source=%s | where balance <= 40000"
                    + " AND balance > 20000 | eval balance_category = \\\"medium\\\"] [search"
                    + " source=%s | where balance <= 20000 | eval balance_category = \\\"low\\\"] |"
                    + " stats count, avg(balance) as avg_bal by balance_category | sort"
                    + " balance_category",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("count", null, "bigint"),
        schema("avg_bal", null, "double"),
        schema("balance_category", null, "string"));

    verifyDataRows(
        result,
        rows(215L, 44775.43720930233, "high"),
        rows(381L, 10699.010498687665, "low"),
        rows(404L, 29732.16584158416, "medium"));
  }

  @Test
  public void testMultisearchWithSubsearchCommands() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where gender = \\\"M\\\" | head 2] "
                    + "[search source=%s | where gender = \\\"F\\\" | head 2] "
                    + "| stats count by gender",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("gender", null, "string"));

    verifyDataRows(result, rows(2L, "F"), rows(2L, "M"));
  }

  @Test
  public void testMultisearchWithDifferentSources() throws IOException {
    // Test multisearch with same source but different filters to simulate different data sources
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where age > 35 | eval source_type = \\\"older\\\"] "
                    + "[search source=%s | where age <= 35 | eval source_type = \\\"younger\\\"] "
                    + "| stats count by source_type | sort source_type",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("source_type", null, "string"));

    verifyDataRows(result, rows(238L, "older"), rows(762L, "younger"));
  }

  @Test
  public void testMultisearchWithMathOperations() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch [search source=%s | where balance > 30000 | eval"
                    + " balance_range = \\\"high\\\"] [search source=%s | where balance <= 30000 |"
                    + " eval balance_range = \\\"normal\\\"] | stats count, min(balance) as"
                    + " min_bal, max(balance) as max_bal by balance_range | sort balance_range",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("count", null, "bigint"),
        schema("min_bal", null, "bigint"),
        schema("max_bal", null, "bigint"),
        schema("balance_range", null, "string"));

    verifyDataRows(result, rows(402L, 30040L, 49989L, "high"), rows(598L, 1011L, 29961L, "normal"));
  }

  @Test
  public void testMultisearchWithSingleSubsearchThrowsError() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | multisearch " + "[search source=%s | where age > 30]",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(exception.getMessage().contains("At least two searches must be specified"));
  }

  // ========================================================================
  // Streaming Command Validation Tests
  // ========================================================================

  @Test
  public void testMultisearchRejectsNonStreamingCommands() {
    // Test that non-streaming commands (stats, sort) are rejected in subsearches
    ResponseException statsException =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | multisearch "
                            + "[search source=%s | stats count by gender] "
                            + "[search source=%s | where age > 30]",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        statsException
            .getMessage()
            .contains("Non-streaming command 'stats' is not supported in multisearch"));

    ResponseException sortException =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | multisearch "
                            + "[search source=%s | sort age desc] "
                            + "[search source=%s | where age > 30]",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT)));

    assertTrue(
        sortException
            .getMessage()
            .contains("Non-streaming command 'sort' is not supported in multisearch"));
  }

  @Test
  public void testMultisearchAllowsStreamingCommands() throws IOException {
    // Test that streaming commands (where, eval, fields, head) work correctly in subsearches
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where age < 30 | eval young = 1 | "
                    + "fields account_number, age, young | head 5] "
                    + "[search source=%s | where age >= 30 | eval senior = 1 | "
                    + "fields account_number, age, senior | head 5] "
                    + "| stats count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"));
    verifyDataRows(result, rows(10L)); // 5 young + 5 senior
  }

  // ========================================================================
  // Event Interleaving Tests
  // ========================================================================

  @Test
  public void testMultisearchEventInterleavingAndSchemaUnification() throws IOException {
    // Test event interleaving and schema unification with different fields
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where age < 25 | "
                    + "eval age_group = \\\"young\\\" | fields account_number, age_group | head 2] "
                    + "[search source=%s | where age > 30 | "
                    + "eval senior_flag = 1 | fields account_number, senior_flag | head 2] "
                    + "| fields account_number, age_group, senior_flag",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    // Verify schema includes fields from both subsearches
    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("age_group", null, "string"),
        schema("senior_flag", null, "int"));

    // Verify we got 4 total rows (2 from each subsearch) and null values are properly handled
    assertTrue("Should have 4 total rows", result.getJSONArray("datarows").length() == 4);
    
    int youngCount = 0, seniorCount = 0;
    for (int i = 0; i < result.getJSONArray("datarows").length(); i++) {
      var row = result.getJSONArray("datarows").getJSONArray(i);
      // Each row should have either age_group or senior_flag, but not both
      if (!row.isNull(1) && row.getString(1).equals("young")) youngCount++;
      if (!row.isNull(2) && row.getInt(2) == 1) seniorCount++;
    }
    assertTrue("Should have 2 young rows", youngCount == 2);
    assertTrue("Should have 2 senior rows", seniorCount == 2);
  }

  @Test
  public void testMultisearchComplexStreamingPipeline() throws IOException {
    // Test complex streaming pipeline with rename, eval, and fields commands
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | multisearch "
                    + "[search source=%s | where balance > 40000 | "
                    + "eval category = \\\"high\\\" | rename account_number as id | head 3] "
                    + "[search source=%s | where balance < 10000 | "
                    + "eval category = \\\"low\\\" | rename account_number as id | head 3] "
                    + "| stats count by category | sort category",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("category", null, "string"));
    verifyDataRows(result, rows(3L, "high"), rows(3L, "low"));
  }
}
