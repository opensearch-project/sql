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

  // ========================================================================
  // Reproduction tests for GitHub issues #5145, #5146, #5147
  // ========================================================================

  /** Reproduce #5145: multisearch without further processing should return all rows. */
  @Test
  public void testMultisearchWithoutFurtherProcessing() throws IOException {
    JSONObject result =
        executeQuery(
            "| multisearch [search source=opensearch-sql_test_index_time_data | where category ="
                + " \\\"A\\\"] [search source=opensearch-sql_test_index_time_data | where category"
                + " = \\\"B\\\"]");

    verifySchema(
        result,
        schema("@timestamp", null, "timestamp"),
        schema("category", null, "string"),
        schema("value", null, "int"),
        schema("timestamp", null, "timestamp"));

    // category A has 26 rows, category B has 25 rows = 51 total
    assertEquals(51, result.getInt("total"));
  }

  /** Reproduce #5146: span expression used after multisearch should work. */
  @Test
  public void testMultisearchWithSpanExpression() throws IOException {
    JSONObject result =
        executeQuery(
            "| multisearch [search source=opensearch-sql_test_index_time_data | where category ="
                + " \\\"A\\\"] [search source=opensearch-sql_test_index_time_data2 | where category"
                + " = \\\"E\\\"] | stats avg(value) by span(@timestamp, 5m)");

    verifySchema(
        result,
        schema("avg(value)", null, "double"),
        schema("span(@timestamp,5m)", null, "timestamp"));

    // Each data point falls in its own 5-min bucket (all >5min apart), so avg = single value
    // Category A: 26 rows from time_test_data, Category E: 10 rows from time_test_data2
    verifyDataRows(
        result,
        // Category A (26 rows)
        rows(8945.0, "2025-07-28 00:15:00"),
        rows(6834.0, "2025-07-28 03:55:00"),
        rows(6589.0, "2025-07-28 07:50:00"),
        rows(9367.0, "2025-07-28 11:05:00"),
        rows(9245.0, "2025-07-28 15:15:00"),
        rows(8917.0, "2025-07-28 19:20:00"),
        rows(8384.0, "2025-07-28 23:30:00"),
        rows(8798.0, "2025-07-29 03:35:00"),
        rows(9306.0, "2025-07-29 07:45:00"),
        rows(8873.0, "2025-07-29 11:50:00"),
        rows(8542.0, "2025-07-29 15:00:00"),
        rows(9321.0, "2025-07-29 19:05:00"),
        rows(8917.0, "2025-07-29 23:10:00"),
        rows(8756.0, "2025-07-30 03:20:00"),
        rows(9234.0, "2025-07-30 07:25:00"),
        rows(8679.0, "2025-07-30 11:35:00"),
        rows(8765.0, "2025-07-30 15:40:00"),
        rows(9187.0, "2025-07-30 19:50:00"),
        rows(8862.0, "2025-07-30 23:55:00"),
        rows(8537.0, "2025-07-31 03:00:00"),
        rows(9318.0, "2025-07-31 07:10:00"),
        rows(8914.0, "2025-07-31 11:15:00"),
        rows(8753.0, "2025-07-31 15:25:00"),
        rows(9231.0, "2025-07-31 19:30:00"),
        rows(8676.0, "2025-07-31 23:40:00"),
        rows(8762.0, "2025-08-01 03:45:00"),
        // Category E (10 rows)
        rows(2001.0, "2025-08-01 04:00:00"),
        rows(2003.0, "2025-08-01 01:00:00"),
        rows(2005.0, "2025-07-31 20:45:00"),
        rows(2007.0, "2025-07-31 16:00:00"),
        rows(2009.0, "2025-07-31 12:30:00"),
        rows(2011.0, "2025-07-31 08:00:00"),
        rows(2013.0, "2025-07-31 04:30:00"),
        rows(2015.0, "2025-07-31 01:00:00"),
        rows(2017.0, "2025-07-30 21:30:00"),
        rows(2019.0, "2025-07-30 18:00:00"));
  }

  /** Reproduce #5147: bin command after multisearch should produce non-null @timestamp. */
  @Test
  public void testMultisearchBinTimestamp() throws IOException {
    JSONObject result =
        executeQuery(
            "| multisearch [search source=opensearch-sql_test_index_time_data | where category ="
                + " \\\"A\\\"] [search source=opensearch-sql_test_index_time_data2 | where category"
                + " = \\\"E\\\"] | fields @timestamp, category, value | bin @timestamp span=5m");

    verifySchema(
        result,
        schema("category", null, "string"),
        schema("value", null, "int"),
        schema("@timestamp", null, "timestamp"));

    // bin floors @timestamp to 5-min boundaries; projectPlusOverriding moves @timestamp to end
    // Category A: 26 rows from time_test_data, Category E: 10 rows from time_test_data2
    verifyDataRows(
        result,
        // Category A (26 rows)
        rows("A", 8945, "2025-07-28 00:15:00"),
        rows("A", 6834, "2025-07-28 03:55:00"),
        rows("A", 6589, "2025-07-28 07:50:00"),
        rows("A", 9367, "2025-07-28 11:05:00"),
        rows("A", 9245, "2025-07-28 15:15:00"),
        rows("A", 8917, "2025-07-28 19:20:00"),
        rows("A", 8384, "2025-07-28 23:30:00"),
        rows("A", 8798, "2025-07-29 03:35:00"),
        rows("A", 9306, "2025-07-29 07:45:00"),
        rows("A", 8873, "2025-07-29 11:50:00"),
        rows("A", 8542, "2025-07-29 15:00:00"),
        rows("A", 9321, "2025-07-29 19:05:00"),
        rows("A", 8917, "2025-07-29 23:10:00"),
        rows("A", 8756, "2025-07-30 03:20:00"),
        rows("A", 9234, "2025-07-30 07:25:00"),
        rows("A", 8679, "2025-07-30 11:35:00"),
        rows("A", 8765, "2025-07-30 15:40:00"),
        rows("A", 9187, "2025-07-30 19:50:00"),
        rows("A", 8862, "2025-07-30 23:55:00"),
        rows("A", 8537, "2025-07-31 03:00:00"),
        rows("A", 9318, "2025-07-31 07:10:00"),
        rows("A", 8914, "2025-07-31 11:15:00"),
        rows("A", 8753, "2025-07-31 15:25:00"),
        rows("A", 9231, "2025-07-31 19:30:00"),
        rows("A", 8676, "2025-07-31 23:40:00"),
        rows("A", 8762, "2025-08-01 03:45:00"),
        // Category E (10 rows)
        rows("E", 2001, "2025-08-01 04:00:00"),
        rows("E", 2003, "2025-08-01 01:00:00"),
        rows("E", 2005, "2025-07-31 20:45:00"),
        rows("E", 2007, "2025-07-31 16:00:00"),
        rows("E", 2009, "2025-07-31 12:30:00"),
        rows("E", 2011, "2025-07-31 08:00:00"),
        rows("E", 2013, "2025-07-31 04:30:00"),
        rows("E", 2015, "2025-07-31 01:00:00"),
        rows("E", 2017, "2025-07-30 21:30:00"),
        rows("E", 2019, "2025-07-30 18:00:00"));
  }

  /** Reproduce #5147 full pattern: bin + stats after multisearch. */
  @Test
  public void testMultisearchBinAndStats() throws IOException {
    JSONObject result =
        executeQuery(
            "| multisearch [search source=opensearch-sql_test_index_time_data | where category ="
                + " \\\"A\\\"] [search source=opensearch-sql_test_index_time_data2 | where category"
                + " = \\\"E\\\"] | bin @timestamp span=5m | stats avg(value) by @timestamp");

    verifySchema(
        result, schema("avg(value)", null, "double"), schema("@timestamp", null, "timestamp"));

    // Each data point falls in its own 5-min bucket (all >5min apart), so avg = single value
    // Category A: 26 rows from time_test_data, Category E: 10 rows from time_test_data2
    verifyDataRows(
        result,
        // Category A (26 rows)
        rows(8945.0, "2025-07-28 00:15:00"),
        rows(6834.0, "2025-07-28 03:55:00"),
        rows(6589.0, "2025-07-28 07:50:00"),
        rows(9367.0, "2025-07-28 11:05:00"),
        rows(9245.0, "2025-07-28 15:15:00"),
        rows(8917.0, "2025-07-28 19:20:00"),
        rows(8384.0, "2025-07-28 23:30:00"),
        rows(8798.0, "2025-07-29 03:35:00"),
        rows(9306.0, "2025-07-29 07:45:00"),
        rows(8873.0, "2025-07-29 11:50:00"),
        rows(8542.0, "2025-07-29 15:00:00"),
        rows(9321.0, "2025-07-29 19:05:00"),
        rows(8917.0, "2025-07-29 23:10:00"),
        rows(8756.0, "2025-07-30 03:20:00"),
        rows(9234.0, "2025-07-30 07:25:00"),
        rows(8679.0, "2025-07-30 11:35:00"),
        rows(8765.0, "2025-07-30 15:40:00"),
        rows(9187.0, "2025-07-30 19:50:00"),
        rows(8862.0, "2025-07-30 23:55:00"),
        rows(8537.0, "2025-07-31 03:00:00"),
        rows(9318.0, "2025-07-31 07:10:00"),
        rows(8914.0, "2025-07-31 11:15:00"),
        rows(8753.0, "2025-07-31 15:25:00"),
        rows(9231.0, "2025-07-31 19:30:00"),
        rows(8676.0, "2025-07-31 23:40:00"),
        rows(8762.0, "2025-08-01 03:45:00"),
        // Category E (10 rows)
        rows(2001.0, "2025-08-01 04:00:00"),
        rows(2003.0, "2025-08-01 01:00:00"),
        rows(2005.0, "2025-07-31 20:45:00"),
        rows(2007.0, "2025-07-31 16:00:00"),
        rows(2009.0, "2025-07-31 12:30:00"),
        rows(2011.0, "2025-07-31 08:00:00"),
        rows(2013.0, "2025-07-31 04:30:00"),
        rows(2015.0, "2025-07-31 01:00:00"),
        rows(2017.0, "2025-07-30 21:30:00"),
        rows(2019.0, "2025-07-30 18:00:00"));
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
