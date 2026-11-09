/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ALIAS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for aggregation functions (MIN, MAX, FIRST, LAST, TAKE) with alias fields.
 * Tests the fix for issue #4595.
 */
public class CalciteAliasFieldAggregationIT extends PPLIntegTestCase {

  private static final String TEST_ALIAS_BUG = "test_alias_bug";

  @Override
  public void init() throws Exception {
    super.init();
    // Excluded on the analytics-engine route from integ-test/build.gradle (alias fields are
    // unsupported there — the raw-PUT index can't even be created and these tests query the alias
    // fields directly), alongside the other AE-route exclusions kept in one place.
    enableCalcite();
    createTestIndexWithAliasFields();
    loadIndex(Index.DATA_TYPE_ALIAS);
  }

  /**
   * Create test index with alias fields mapping and insert sample data. This mirrors the
   * reproduction steps from issue #4595.
   */
  private void createTestIndexWithAliasFields() throws IOException {
    // Delete the index if it exists (for test isolation)
    try {
      Request deleteIndex = new Request("DELETE", "/" + TEST_ALIAS_BUG);
      client().performRequest(deleteIndex);
    } catch (ResponseException e) {
      // Index doesn't exist, which is fine
    }

    // Create index with alias fields
    Request createIndex = new Request("PUT", "/" + TEST_ALIAS_BUG);
    createIndex.setJsonEntity(
        "{\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"created_at\": {\"type\": \"date\"},\n"
            + "      \"@timestamp\": {\"type\": \"alias\", \"path\": \"created_at\"},\n"
            + "      \"value\": {\"type\": \"integer\"},\n"
            + "      \"value_alias\": {\"type\": \"alias\", \"path\": \"value\"}\n"
            + "    }\n"
            + "  }\n"
            + "}");
    client().performRequest(createIndex);

    // Insert test documents
    Request bulkRequest = new Request("POST", "/" + TEST_ALIAS_BUG + "/_bulk?refresh=true");
    bulkRequest.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"created_at\": \"2024-01-01T10:00:00Z\", \"value\": 100}\n"
            + "{\"index\":{}}\n"
            + "{\"created_at\": \"2024-01-02T10:00:00Z\", \"value\": 200}\n"
            + "{\"index\":{}}\n"
            + "{\"created_at\": \"2024-01-03T10:00:00Z\", \"value\": 300}\n");
    client().performRequest(bulkRequest);
  }

  @Test
  public void testMinWithDateAliasField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats MIN(@timestamp)", TEST_ALIAS_BUG));
    verifySchema(actual, schema("MIN(@timestamp)", "timestamp"));
    verifyDataRows(actual, rows("2024-01-01 10:00:00"));
  }

  @Test
  public void testMaxWithDateAliasField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats MAX(@timestamp)", TEST_ALIAS_BUG));
    verifySchema(actual, schema("MAX(@timestamp)", "timestamp"));
    verifyDataRows(actual, rows("2024-01-03 10:00:00"));
  }

  @Test
  public void testMinMaxWithNumericAliasField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats MIN(value_alias), MAX(value_alias)", TEST_ALIAS_BUG));
    verifySchemaInOrder(
        actual, schema("MIN(value_alias)", "int"), schema("MAX(value_alias)", "int"));
    verifyDataRows(actual, rows(100, 300));
  }

  @Test
  public void testFirstWithAliasField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | sort @timestamp | stats FIRST(@timestamp)", TEST_ALIAS_BUG));
    verifySchema(actual, schema("FIRST(@timestamp)", "timestamp"));
    assertTimestampMember(actual.getJSONArray("datarows").getJSONArray(0).get(0));
  }

  @Test
  public void testLastWithAliasField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | sort @timestamp | stats LAST(@timestamp)", TEST_ALIAS_BUG));
    verifySchema(actual, schema("LAST(@timestamp)", "timestamp"));
    assertTimestampMember(actual.getJSONArray("datarows").getJSONArray(0).get(0));
  }

  @Test
  public void testTakeWithAliasField() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort @timestamp | stats TAKE(@timestamp, 2)", TEST_ALIAS_BUG));
    verifySchema(actual, schema("TAKE(@timestamp, 2)", "array"));
    assertTimestampTakeMembers(actual.getJSONArray("datarows").getJSONArray(0).getJSONArray(0));
  }

  @Test
  public void testAggregationsWithOriginalFieldsStillWork() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats MIN(created_at), MAX(value)", TEST_ALIAS_BUG));
    verifySchemaInOrder(
        actual, schema("MIN(created_at)", "timestamp"), schema("MAX(value)", "int"));
    verifyDataRows(actual, rows("2024-01-01 10:00:00", 300));
  }

  @Test
  public void testUnaffectedAggregationsWithAliasFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats SUM(value_alias), AVG(value_alias), COUNT(value_alias)",
                TEST_ALIAS_BUG));
    verifySchemaInOrder(
        actual,
        schema("SUM(value_alias)", "bigint"),
        schema("AVG(value_alias)", "double"),
        schema("COUNT(value_alias)", "bigint"));
    verifyDataRows(actual, rows(600, 200.0, 3));
  }

  @Test
  public void testAliasTypeWithLastFirstTakeLatestEarliestAggregation() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats take(original_text, 2), last(original_text),"
                    + " first(original_text), take(alias_text, 2), last(alias_text),"
                    + " first(alias_text), take(original_col, 2), last(original_col),"
                    + " first(original_col), take(alias_col, 2), last(alias_col), first(alias_col),"
                    + " latest(original_col), earliest(original_col), latest(alias_col),"
                    + " earliest(alias_col), latest(original_text), earliest(original_text),"
                    + " latest(alias_text), earliest(alias_text)",
                TEST_INDEX_ALIAS));

    JSONArray row = actual.getJSONArray("datarows").getJSONArray(0);
    Set<String> texts = Set.of("a b c", "d e f", "x y z");
    Set<Integer> numbers = Set.of(1, 2, 3);
    assertTakeMembers(row.getJSONArray(0), 2, texts);
    assertMember(row.get(1), texts);
    assertMember(row.get(2), texts);
    assertTakeMembers(row.getJSONArray(3), 2, texts);
    assertMember(row.get(4), texts);
    assertMember(row.get(5), texts);
    assertTakeMembers(row.getJSONArray(6), 2, numbers);
    assertMember(row.get(7), numbers);
    assertMember(row.get(8), numbers);
    assertTakeMembers(row.getJSONArray(9), 2, numbers);
    assertMember(row.get(10), numbers);
    assertMember(row.get(11), numbers);

    assertEquals(3, row.getInt(12));
    assertEquals(1, row.getInt(13));
    assertEquals(3, row.getInt(14));
    assertEquals(1, row.getInt(15));
    assertEquals("x y z", row.getString(16));
    assertEquals("a b c", row.getString(17));
    assertEquals("x y z", row.getString(18));
    assertEquals("a b c", row.getString(19));
  }

  private static void assertMember(Object actual, Set<?> expectedValues) {
    assertTrue(
        "Expected one of " + expectedValues + " but got " + actual,
        expectedValues.contains(actual));
  }

  private static void assertTakeMembers(JSONArray actual, int expectedSize, Set<?> expectedValues) {
    assertEquals(expectedSize, actual.length());
    Set<Object> distinct = new HashSet<>();
    for (int i = 0; i < actual.length(); i++) {
      Object value = actual.get(i);
      assertMember(value, expectedValues);
      distinct.add(value);
    }
    assertEquals(expectedSize, distinct.size());
  }

  private static String normalizeTimestamp(Object value) {
    return value.toString().replace('T', ' ').replace(".000Z", "").replace("Z", "");
  }

  private static void assertTimestampMember(Object actual) {
    assertMember(
        normalizeTimestamp(actual),
        Set.of("2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-03 10:00:00"));
  }

  private static void assertTimestampTakeMembers(JSONArray actual) {
    assertEquals(2, actual.length());
    Set<String> normalized = new HashSet<>();
    for (int i = 0; i < actual.length(); i++) {
      String value = normalizeTimestamp(actual.get(i));
      assertTimestampMember(value);
      normalized.add(value);
    }
    assertEquals(2, normalized.size());
  }
}
