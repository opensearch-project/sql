/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
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
}
