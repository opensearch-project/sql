/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteReverseCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.BANK);
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.STATE_COUNTRY);
  }

  @Test
  public void testReverse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields account_number | sort account_number | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testReverseWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields account_number, firstname | sort account_number | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(32, "Dillard"),
        rows(25, "Virginia"),
        rows(20, "Elinor"),
        rows(18, "Dale"),
        rows(13, "Nanette"),
        rows(6, "Hattie"),
        rows(1, "Amber JOHnny"));
  }

  @Test
  public void testReverseWithSort() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | fields account_number | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testDoubleReverse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields account_number | sort account_number | reverse | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testReverseWithHead() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields account_number | sort account_number | reverse | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(result, rows(32), rows(25), rows(20));
  }

  @Test
  public void testReverseWithComplexPipeline() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where account_number > 18 | fields account_number | sort"
                    + " account_number | reverse | head 2",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(result, rows(32), rows(25));
  }

  @Test
  public void testReverseWithDescendingSort() throws IOException {
    // Test reverse with descending sort (- age)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testReverseWithMixedSortDirections() throws IOException {
    // Test reverse with mixed sort directions (- age, + firstname)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort - account_number, + firstname | fields account_number, firstname"
                    + " | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(1, "Amber JOHnny"),
        rows(6, "Hattie"),
        rows(13, "Nanette"),
        rows(18, "Dale"),
        rows(20, "Elinor"),
        rows(25, "Virginia"),
        rows(32, "Dillard"));
  }

  @Test
  public void testDoubleReverseWithDescendingSort() throws IOException {
    // Test double reverse with descending sort (- age)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number | reverse | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        result, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testDoubleReverseWithMixedSortDirections() throws IOException {
    // Test double reverse with mixed sort directions (- age, + firstname)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort - account_number, + firstname | fields account_number, firstname"
                    + " | reverse | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(32, "Dillard"),
        rows(25, "Virginia"),
        rows(20, "Elinor"),
        rows(18, "Dale"),
        rows(13, "Nanette"),
        rows(6, "Hattie"),
        rows(1, "Amber JOHnny"));
  }

  @Test
  public void testReverseIgnoredWithoutSortOrTimestamp() throws IOException {
    // Test that reverse is ignored when there's no explicit sort and no @timestamp field
    // BANK index doesn't have @timestamp, so reverse should be ignored
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields account_number | reverse | head 3", TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"));
    // Without sort or @timestamp, reverse is ignored, so data comes in natural order
    // The first 3 documents in natural order (ascending by account_number)
    verifyDataRowsInOrder(result, rows(1), rows(6), rows(13));
  }

  @Test
  public void testReverseWithTimestampField() throws IOException {
    // Test that reverse with @timestamp field sorts by @timestamp DESC
    // TIME_TEST_DATA index has @timestamp field
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields value, category, `@timestamp` | reverse | head 5",
                TEST_INDEX_TIME_DATA));
    verifySchema(
        result,
        schema("value", "int"),
        schema("category", "string"),
        schema("@timestamp", "timestamp"));
    // Should return the latest 5 records (highest @timestamp values) in descending order
    // Based on the test data, these are IDs 100, 99, 98, 97, 96
    verifyDataRowsInOrder(
        result,
        rows(8762, "A", "2025-08-01 03:47:41"),
        rows(7348, "C", "2025-08-01 02:00:56"),
        rows(9015, "B", "2025-08-01 01:14:11"),
        rows(6489, "D", "2025-08-01 00:27:26"),
        rows(8676, "A", "2025-07-31 23:40:33"));
  }

  @Test
  public void testReverseWithTimestampAndExplicitSort() throws IOException {
    // Test that explicit sort takes precedence over @timestamp
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields value, category | sort value | reverse | head 3",
                TEST_INDEX_TIME_DATA));
    verifySchema(result, schema("value", "int"), schema("category", "string"));
    // Should reverse the value sort, giving us the highest values
    verifyDataRowsInOrder(result, rows(9521, "B"), rows(9367, "A"), rows(9321, "A"));
  }

  @Test
  public void testStreamstatsWithReverse() throws IOException {
    // Test that reverse is ignored when used directly after streamstats
    // streamstats maintains order via __stream_seq__, but this field is projected out
    // and doesn't create a detectable collation, so reverse is ignored (no-op)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg | reverse",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"));
    // Reverse is ignored, so data remains in original streamstats order
    verifyDataRowsInOrder(
        result,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25));
  }

  @Test
  public void testStreamstatsWindowWithReverse() throws IOException {
    // Test that reverse is ignored after streamstats with window
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | streamstats window=2 avg(age) as avg | reverse",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("avg", "double"));
    // Reverse is ignored, data remains in original order
    // Window=2 means average of current and previous row (sliding window of size 2)
    verifyDataRowsInOrder(
        result,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 27.5),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5));
  }

  @Test
  public void testStreamstatsByWithReverse() throws IOException {
    // Test that reverse is ignored after streamstats with partitioning (by clause)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg by country | reverse",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"));
    // With backtracking, reverse now works and reverses the __stream_seq__ order
    verifyDataRowsInOrder(
        result,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50),
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70));
  }

  @Test
  public void testStreamstatsWithSortThenReverse() throws IOException {
    // Test that reverse works when there's an explicit sort after streamstats
    // The explicit sort creates a collation that reverse can detect and reverse
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt | sort age | reverse | head 3",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"));
    // With explicit sort and reverse, data is in descending age order
    verifyDataRowsInOrder(
        result,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3));
  }

  // ==================== Tests for blocking operators ====================
  // These tests verify that reverse is a no-op after blocking operators
  // that destroy collation (aggregate, join, window functions).

  @Test
  public void testReverseAfterAggregationIsNoOp() throws IOException {
    // Test that reverse is a no-op after aggregation (stats)
    // Aggregation destroys input ordering, so reverse has no collation to reverse
    // and BANK index has no @timestamp, so reverse should be ignored
    JSONObject result =
        executeQuery(
            String.format("source=%s | stats count() as c by gender | reverse", TEST_INDEX_BANK));
    verifySchema(result, schema("c", "bigint"), schema("gender", "string"));
    // Data should be in aggregation order (no reverse applied)
    // Use verifyDataRows (unordered) since aggregation order is not guaranteed
    verifyDataRows(result, rows(4, "M"), rows(3, "F"));
  }

  @Test
  public void testReverseAfterAggregationWithSort() throws IOException {
    // Test that reverse works when there's an explicit sort after aggregation
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by gender | sort gender | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("c", "bigint"), schema("gender", "string"));
    // With explicit sort and reverse, data should be in descending gender order
    verifyDataRowsInOrder(result, rows(4, "M"), rows(3, "F"));
  }

  @Test
  public void testReverseSortAggregationIsNoOp() throws IOException {
    // Test that sort before aggregation doesn't allow reverse after aggregation
    // Even with sort before stats, aggregation destroys the collation
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | stats count() as c by gender | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("c", "bigint"), schema("gender", "string"));
    // Reverse is a no-op because aggregation destroyed the sort collation
    // Use verifyDataRows (unordered) since aggregation order is not guaranteed
    verifyDataRows(result, rows(4, "M"), rows(3, "F"));
  }

  @Test
  public void testReverseAfterWhereWithSort() throws IOException {
    // Test that reverse works through filter (where) to find the sort
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | where balance > 30000 | fields account_number,"
                    + " balance | reverse",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("balance", "bigint"));
    // Reverse should work through the filter to reverse the sort
    verifyDataRowsInOrder(
        result, rows(32, 48086), rows(25, 40540), rows(18, 35983), rows(13, 32838));
  }

  @Test
  public void testReverseAfterEvalWithSort() throws IOException {
    // Test that reverse works through eval (project) to find the sort
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | eval double_balance = balance * 2 | fields"
                    + " account_number, double_balance | reverse | head 3",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("double_balance", "bigint"));
    // Reverse should work through eval to reverse the sort
    verifyDataRowsInOrder(result, rows(32, 96172), rows(25, 81080), rows(20, 73438));
  }

  @Test
  public void testReverseAfterMultipleFilters() throws IOException {
    // Test that reverse works through multiple filters
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | where balance > 20000 | where age > 30 | fields"
                    + " account_number, balance, age | reverse",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("balance", "bigint"),
        schema("age", "int"));
    // Reverse should work through multiple filters
    verifyDataRowsInOrder(result, rows(32, 48086, 39), rows(25, 40540, 36), rows(18, 35983, 33));
  }

  @Test
  public void testReverseWithTimestampAfterAggregation() throws IOException {
    // Test that reverse uses @timestamp when aggregation destroys collation
    // TIME_TEST_DATA has @timestamp field
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by category | reverse", TEST_INDEX_TIME_DATA));
    verifySchema(result, schema("c", "bigint"), schema("category", "string"));
    // Even though aggregation destroys collation, there's no @timestamp in the
    // aggregated result, so reverse is a no-op
    // Use verifyDataRows (unordered) since aggregation order is not guaranteed
    verifyDataRows(result, rows(25, "A"), rows(25, "B"), rows(25, "C"), rows(25, "D"));
  }
}
