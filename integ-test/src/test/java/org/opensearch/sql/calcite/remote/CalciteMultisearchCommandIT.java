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
    loadIndex(Index.LOCATIONS_TYPE_CONFLICT);
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
    JSONObject result =
        executeQuery(
            "| multisearch [search"
                + " source=opensearch-sql_test_index_time_data | where category IN (\\\"A\\\","
                + " \\\"B\\\")] [search source=opensearch-sql_test_index_time_data2 | where"
                + " category IN (\\\"E\\\", \\\"F\\\")] | head 10");

    verifySchema(
        result,
        schema("@timestamp", null, "timestamp"),
        schema("category", null, "string"),
        schema("value", null, "int"),
        schema("timestamp", null, "timestamp"));

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

    assertTrue(
        "Error message should indicate minimum subsearch requirement",
        exception.getMessage().contains("Multisearch command requires at least two subsearches"));
  }

  @Test
  public void testMultisearchWithDifferentIndicesSchemaMerge() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where age > 35 | fields account_number,"
                    + " firstname, balance] [search source=%s | where age > 35 | fields"
                    + " account_number, balance] | stats count() as total_count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(result, schema("total_count", null, "bigint"));
    verifyDataRows(result, rows(241L));
  }

  @Test
  public void testMultisearchWithMixedIndicesComplexSchemaMerge() throws IOException {
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
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | where category = \\\"A\\\" | stats count() as count_a] "
                    + "[search source=%s | where category = \\\"E\\\" | stats count() as count_e] "
                    + "| stats sum(count_a) as total_a, sum(count_e) as total_e",
                "opensearch-sql_test_index_time_data", "opensearch-sql_test_index_time_data2"));

    verifySchema(result, schema("total_a", null, "bigint"), schema("total_e", null, "bigint"));
    verifyDataRows(result, rows(26L, 10L));
  }

  @Test
  public void testMultisearchNullFillingForMissingFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where account_number = 1 | fields firstname,"
                    + " age, balance] [search source=%s | where account_number = 1 | fields"
                    + " lastname, city, employer] | head 2",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("firstname", null, "string"),
        schema("age", null, "bigint"),
        schema("balance", null, "bigint"),
        schema("lastname", null, "string"),
        schema("city", null, "string"),
        schema("employer", null, "string"));

    verifyDataRows(
        result,
        rows("Amber", 32L, 39225L, null, null, null),
        rows(null, null, null, "Duke", "Brogan", "Pyrami"));
  }

  @Test
  public void testMultisearchNullFillingAcrossIndices() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [search source=%s | where account_number = 1 | fields"
                    + " account_number, firstname, balance] [search source=%s | where"
                    + " account_number = 1 | fields city, employer, email] | head 2",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("firstname", null, "string"),
        schema("balance", null, "bigint"),
        schema("city", null, "string"),
        schema("employer", null, "string"),
        schema("email", null, "string"));

    verifyDataRows(
        result,
        rows(1L, "Amber", 39225L, null, null, null),
        rows(null, null, null, "Brogan", "Pyrami", "amberduke@pyrami.com"));
  }

  @Test
  public void testMultisearchWithDirectTypeConflict() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch "
                            + "[search source=%s | fields firstname, age, balance | head 2] "
                            + "[search source=%s | fields description, age, place_id | head 2]",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_LOCATIONS_TYPE_CONFLICT)));

    assertTrue(
        "Error message should indicate type conflict",
        exception
            .getMessage()
            .contains("Unable to process column 'age' due to incompatible types:"));
  }

  @Test
  public void testMultisearchCrossIndexFieldSelection() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch "
                    + "[search source=%s | fields firstname, balance | head 2] "
                    + "[search source=%s | fields description, place_id | head 2]",
                TEST_INDEX_ACCOUNT, TEST_INDEX_LOCATIONS_TYPE_CONFLICT));

    verifySchema(
        result,
        schema("firstname", null, "string"),
        schema("balance", null, "bigint"),
        schema("description", null, "string"),
        schema("place_id", null, "int"));

    verifyDataRows(
        result,
        rows("Amber", 39225L, null, null),
        rows("Hattie", 5686L, null, null),
        rows(null, null, "Central Park", 1001),
        rows(null, null, "Times Square", 1002));
  }

  @Test
  public void testMultisearchTypeConflictWithStats() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| multisearch "
                            + "[search source=%s | fields age] "
                            + "[search source=%s | fields age] "
                            + "| stats count() as total",
                        TEST_INDEX_ACCOUNT, TEST_INDEX_LOCATIONS_TYPE_CONFLICT)));

    assertTrue(
        "Error message should indicate type conflict",
        exception
            .getMessage()
            .contains("Unable to process column 'age' due to incompatible types:"));
  }
}
