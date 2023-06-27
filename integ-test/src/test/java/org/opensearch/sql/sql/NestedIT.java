/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_WITH_NULLS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.jupiter.api.Disabled;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.MULTI_NESTED);
    loadIndex(Index.NESTED);
    loadIndex(Index.NESTED_WITHOUT_ARRAYS);
    loadIndex(Index.EMPLOYEE_NESTED);
    loadIndex(Index.NESTED_WITH_NULLS);
  }

  @Test
  public void nested_function_with_array_of_nested_field_test() {
    String query = "SELECT nested(message.info), nested(comment.data) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("c", "ab"),
        rows("a", "ab"),
        rows("b", "aa"),
        rows("c", "aa"),
        rows("a", "ab"),
        rows("zz", new JSONArray(List.of("aa", "bb"))));
  }

  @Test
  public void nested_function_in_select_test() {
    String query = "SELECT nested(message.info), nested(comment.data), "
        + "nested(message.dayOfWeek) FROM "
        + TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(5, result.getInt("total"));
    verifySchema(result,
        schema("nested(message.info)", null, "keyword"),
        schema("nested(comment.data)", null, "keyword"),
        schema("nested(message.dayOfWeek)", null, "long"));
    verifyDataRows(result,
        rows("a", "ab", 1),
        rows("b", "aa", 2),
        rows("c", "aa", 1),
        rows("c", "ab", 4),
        rows("zz", "bb", 6));
  }

  // Has to be tested with JSON format when https://github.com/opensearch-project/sql/issues/1317
  // gets resolved
  @Disabled // TODO fix me when aggregation is supported
  public void nested_function_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(14));
  }

  // TODO Enable me when nested aggregation is supported
  @Disabled
  public void nested_function_with_arrays_in_an_aggregate_function_in_select_test() {
    String query = "SELECT sum(nested(message.dayOfWeek)) FROM " +
        TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result, rows(19));
  }

  // TODO not currently supported by legacy, should we add implementation in AstBuilder?
  @Disabled
  public void nested_function_in_a_function_in_select_test() {
    String query = "SELECT upper(nested(message.info)) FROM " +
        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
    JSONObject result = executeJdbcRequest(query);

    verifyDataRows(result,
        rows("A"),
        rows("B"),
        rows("C"),
        rows("C"),
        rows("ZZ"));
  }

  @Test
  public void nested_function_with_array_of_multi_nested_field_test() {
    String query = "SELECT nested(message.author.name) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("e"),
        rows("f"),
        rows("g"),
        rows("h"),
        rows("p"),
        rows("yy"));
  }

  @Test
  public void nested_function_with_null_and_missing_fields_test() {
    String query = "SELECT nested(message.info), nested(comment.data) FROM "
        + TEST_INDEX_NESTED_WITH_NULLS;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(10, result.getInt("total"));
    verifyDataRows(result,
        rows(null, "hh"),
        rows("b", "aa"),
        rows("c", "aa"),
        rows("c", "ab"),
        rows("a", "ab"),
        rows("zz", new JSONArray(List.of("aa", "bb"))),
        rows("zz", new JSONArray(List.of("aa", "bb"))),
        rows(null, "ee"),
        rows("a", "ab"),
        rows("rr", new JSONArray(List.of("asdf", "sdfg"))));
  }

  @Test
  public void nested_function_multiple_fields_with_matched_and_mismatched_paths_test() {
    String query =
        "SELECT nested(message.author), nested(message.dayOfWeek), nested(message.info), nested(comment.data), "
            + "nested(comment.likes) FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("e", 1, "a", "ab", 3),
        rows("f", 2, "b", "aa", 2),
        rows("g", 1, "c", "aa", 3),
        rows("h", 4, "c", "ab", 1),
        rows("i", 5, "a", "ab", 1),
        rows("zz", 6, "zz", new JSONArray(List.of("aa", "bb")), 10));
  }

  @Test
  public void nested_function_mixed_with_non_nested_type_test() {
    String query =
        "SELECT nested(message.info), someField FROM " + TEST_INDEX_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("a", "b"),
        rows("b", "a"),
        rows("c", "a"),
        rows("c", "b"),
        rows("a", "b"),
        rows("zz", "a"));
  }

  @Test
  public void nested_function_with_order_by_clause() {
    String query =
        "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE
            + " ORDER BY nested(message.info)";
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("a"),
        rows("c"),
        rows("a"),
        rows("b"),
        rows("c"),
        rows("zz"));
  }

  @Test
  public void nested_function_with_order_by_clause_desc() {
    String query =
        "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE
            + " ORDER BY nested(message.info, message) DESC";
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("zz"),
        rows("c"),
        rows("c"),
        rows("a"),
        rows("b"),
        rows("a"));
  }

  @Test
  public void nested_function_and_field_with_order_by_clause() {
    String query =
        "SELECT nested(message.info), myNum FROM " + TEST_INDEX_NESTED_TYPE
            + " ORDER BY nested(message.info, message), myNum";
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("a", 1),
        rows("c", 4),
        rows("a", 4),
        rows("b", 2),
        rows("c", 3),
        rows("zz", new JSONArray(List.of(3, 4))));
  }

  // Nested function in GROUP BY clause is not yet implemented for JDBC format. This test ensures
  // that the V2 engine falls back to legacy implementation.
  // TODO Fix the test when NESTED is supported in GROUP BY in the V2 engine.
  @Test
  public void nested_function_with_group_by_clause() {
    String query =
        "SELECT count(*) FROM " + TEST_INDEX_NESTED_TYPE + " GROUP BY nested(message.info)";
    JSONObject result = executeJdbcRequest(query);

    assertTrue(result.getJSONObject("error").get("details").toString().contains(
        "Aggregation type nested is not yet implemented"
    ));
  }

  // Nested function in HAVING clause is not yet implemented for JDBC format. This test ensures
  // that the V2 engine falls back to legacy implementation.
  // TODO Fix the test when NESTED is supported in HAVING in the V2 engine.
  @Test
  public void nested_function_with_having_clause() {
    String query =
        "SELECT count(*) FROM " + TEST_INDEX_NESTED_TYPE + " GROUP BY myNum HAVING nested(comment.likes) > 7";
    JSONObject result = executeJdbcRequest(query);

    assertTrue(result.getJSONObject("error").get("details").toString().contains(
        "For more details, please send request for Json format to see the raw response from OpenSearch engine."
    ));
  }

  @Test
  public void nested_function_mixed_with_non_nested_types_test() {
    String query =
        "SELECT nested(message.info), office, office.west FROM " + TEST_INDEX_MULTI_NESTED_TYPE;
    JSONObject result = executeJdbcRequest(query);

    assertEquals(6, result.getInt("total"));
    verifyDataRows(result,
        rows("a",
            new JSONObject(Map.of("south", 3, "west", "ab")), "ab"),
        rows("b",
            new JSONObject(Map.of("south", 5, "west", "ff")), "ff"),
        rows("c",
            new JSONObject(Map.of("south", 3, "west", "ll")), "ll"),
        rows("d", null, null),
        rows("i", null, null),
        rows("zz", null, null));
  }

  @Test
  public void nested_function_with_relevance_query() {
    String query =
        "SELECT nested(message.info), highlight(someField) FROM "
            + TEST_INDEX_NESTED_TYPE + " WHERE match(someField, 'b')";
    JSONObject result = executeJdbcRequest(query);

    assertEquals(3, result.getInt("total"));
    verifyDataRows(result,
        rows("a", new JSONArray(List.of("<em>b</em>"))),
        rows("c", new JSONArray(List.of("<em>b</em>"))),
        rows("a", new JSONArray(List.of("<em>b</em>"))));
  }

  @Test
  public void nested_with_non_nested_type_test() {
    String query = "SELECT nested(someField) FROM " + TEST_INDEX_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains(
            "{\n" +
            "  \"error\": {\n" +
            "    \"reason\": \"Invalid SQL query\",\n" +
            "    \"details\": \"Illegal nested field name: someField\",\n" +
            "    \"type\": \"IllegalArgumentException\"\n" +
            "  },\n" +
            "  \"status\": 400\n" +
            "}"
    ));
  }

  @Test
  public void nested_missing_path() {
    String query = "SELECT nested(message.invalid) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains("" +
        "{\n" +
        "  \"error\": {\n" +
        "    \"reason\": \"Invalid SQL query\",\n" +
        "    \"details\": \"can't resolve Symbol(namespace=FIELD_NAME, name=message.invalid) in type env\",\n" +
        "    \"type\": \"SemanticCheckException\"\n" +
        "  },\n" +
        "  \"status\": 400\n" +
        "}"
    ));
  }

  @Test
  public void nested_missing_path_argument() {
    String query = "SELECT nested(message.author.name, invalid) FROM " + TEST_INDEX_MULTI_NESTED_TYPE;

    Exception exception = assertThrows(RuntimeException.class,
        () -> executeJdbcRequest(query));
    assertTrue(exception.getMessage().contains("" +
        "{\n" +
        "  \"error\": {\n" +
        "    \"reason\": \"Invalid SQL query\",\n" +
        "    \"details\": \"can't resolve Symbol(namespace=FIELD_NAME, name=invalid) in type env\",\n" +
        "    \"type\": \"SemanticCheckException\"\n" +
        "  },\n" +
        "  \"status\": 400\n" +
        "}"
    ));
  }

  @Test
  public void test_nested_where_with_and_conditional() {
    String query = "SELECT nested(message.info), nested(message.author) FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message, message.info = 'a' AND message.author = 'e')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows("a", "e"));
  }

  @Test
  public void test_nested_in_select_and_where_as_predicate_expression() {
    String query = "SELECT nested(message.info) FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message.info) = 'a'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(3, result.getInt("total"));
    verifyDataRows(
        result,
        rows("a"),
        rows("c"),
        rows("a")
    );
  }

  @Test
  public void test_nested_in_where_as_predicate_expression() {
    String query = "SELECT message.info FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message.info) = 'a'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    // Only first index of array is returned. Second index has 'a'
    verifyDataRows(result, rows("a"), rows("c"));
  }

  @Test
  public void test_nested_in_where_as_predicate_expression_with_like() {
    String query = "SELECT message.info FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message.info) LIKE 'a'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    // Only first index of array is returned. Second index has 'a'
    verifyDataRows(result, rows("a"), rows("c"));
  }

  @Test
  public void test_nested_in_where_as_predicate_expression_with_multiple_conditions() {
    String query = "SELECT message.info, comment.data, message.dayOfWeek FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(message.info) = 'zz' OR nested(comment.data) = 'ab' AND nested(message.dayOfWeek) >= 4";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifyDataRows(
        result,
        rows("c", "ab", 4),
        rows("zz", "aa", 6)
    );
  }

  @Test
  public void test_nested_in_where_as_predicate_expression_with_relevance_query() {
    String query = "SELECT comment.likes, someField FROM " + TEST_INDEX_NESTED_TYPE
        + " WHERE nested(comment.likes) = 10 AND match(someField, 'a')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(1, result.getInt("total"));
    verifyDataRows(result, rows(10, "a"));
  }
}
