/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC_FIELDS = "test_dynamic_fields";
  private static final String TEST_INDEX_COMPLEX_JSON = "test_complex_json";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.JSON_TEST);

    createDynamicFieldsTestData();
    createComplexJsonTestData();
  }

  private void createDynamicFieldsTestData() throws IOException {
    createDocumentsWithJsonField(
        TEST_INDEX_DYNAMIC_FIELDS,
        "json_data",
        "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}",
        "{\"name\": \"Jane\", \"age\": 25, \"country\": \"USA\"}",
        "{\"product\": \"laptop\", \"price\": 999.99, \"brand\": \"Dell\"}");
  }

  private void createComplexJsonTestData() throws IOException {
    createDocumentsWithJsonField(
        TEST_INDEX_COMPLEX_JSON,
        "data",
        "{\"user\": {\"name\": \"Alice\", \"profile\": {\"age\": 28, \"location\": \"Seattle\"}},"
            + " \"preferences\": [\"music\", \"travel\"]}",
        "{\"user\": {\"name\": \"Bob\", \"profile\": {\"age\": 35, \"location\": \"Portland\"}},"
            + " \"settings\": {\"theme\": \"dark\", \"notifications\": true}}",
        "{\"user\": {\"name\": \"John\", \"profile\": {\"age\": 40, \"location\": \"Kirkland\"}},"
            + " \"nested\": [{\"a\": \"v1\", \"arr\": [1, 2, 3]}, {\"a\": \"v2\", \"arr\": [4,"
            + " 5]}]}");
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "spath input=json_data output=result path=name | fields id, result"));
    verifySchema(result, schema("id", "bigint"), schema("result", "string"));
    verifyDataRows(result, rows(1L, "John"), rows(2L, "Jane"), rows(3L, "null"));
  }

  @Test
  public void testSpathDynamicFieldsBasic() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS, "spath input=json_data | fields id, name, age, city"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"),
        schema("city", "string"));

    verifyDataRows(
        result,
        rows(1, "John", 30, "New York"),
        rows(2, "Jane", 25, null),
        rows(3, null, null, null));
  }

  @Test
  public void testSpathDynamicFieldsWithDifferentFields() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "spath input=json_data | fields id, name, country, product, price"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("country", "string"),
        schema("product", "string"),
        schema("price", "double"));

    verifyDataRows(
        result,
        rows(1, "John", null, null, null),
        rows(2, "Jane", "USA", null, null),
        rows(3, null, null, "laptop", 999.99));
  }

  @Test
  public void testSpathDynamicFieldsWithFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "spath input=json_data | where name = 'John' | fields id, name, age"));

    verifySchema(result, schema("id", "bigint"), schema("name", "string"), schema("age", "int"));
    verifyDataRows(result, rows(1, "John", 30));
  }

  @Test
  public void testSpathDynamicFieldsWithAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS, "spath input=json_data | stats count() as total_count"));

    verifySchema(result, schema("total_count", "bigint"));
    verifyDataRows(result, rows(3));
  }

  @Test
  public void testSpathDynamicFieldsWithSorting() throws IOException {
    JSONObject result =
        executeQuery(
            source(TEST_INDEX_DYNAMIC_FIELDS, "spath input=json_data | sort id | fields id, name"));

    verifySchema(result, schema("id", "bigint"), schema("name", "string"));
    verifyDataRows(result, rows(1, "John"), rows(2, "Jane"), rows(3, null));
  }

  @Test
  public void testSpathAndSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "spath input=json_data | spath input=json_data | fields id, name | head 1"));

    System.out.println(result.toString(2)); // TODO: To be deleted

    verifySchema(result, schema("id", "bigint"), schema("name", "array"));

    verifyDataRows(result, rows(1, arr("John", "John")));
  }

  @Test
  public void testFieldsSpathSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "fields id, json_data | spath input=json_data | spath input=json_data"
                    + "| fields id, name | head 1"));

    verifySchema(result, schema("id", "bigint"), schema("name", "array"));

    verifyDataRows(result, rows(1, arr("John", "John")));
  }

  @Test
  public void testSpathAndStats() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_FIELDS,
                "spath input=json_data | stats count() as name_count by name"
                    + " | fields name_count, name"));

    verifySchema(result, schema("name_count", "bigint"), schema("name", "string"));
    verifyDataRows(result, rows(1L, null), rows(1L, "Jane"), rows(1L, "John"));
  }

  @Test
  public void testSpathDynamicFieldsExplain() throws IOException {
    executeQuery(
        source(TEST_INDEX_DYNAMIC_FIELDS, "spath input=json_data | fields id, name, age, city"));
  }

  @Test
  public void testSpathWithComplexJsonNestedFieldAccess() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | fields id, user.name, user.profile.age,"
                    + " user.profile.location"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("user.name", "string"),
        schema("user.profile.age", "int"),
        schema("user.profile.location", "string"));
    verifyDataRows(
        result,
        rows(1L, "Alice", 28, "Seattle"),
        rows(2L, "Bob", 35, "Portland"),
        rows(3L, "John", 40, "Kirkland"));
  }

  @Test
  public void testSpathWithComplexJsonFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | where isnotnull(user.name) | fields id, user.name"));

    verifySchema(result, schema("id", "bigint"), schema("user.name", "string"));
    verifyDataRows(result, rows(1L, "Alice"), rows(2L, "Bob"), rows(3L, "John"));
  }

  @Test
  public void testSpathWithComplexJsonAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(TEST_INDEX_COMPLEX_JSON, "spath input=data | stats count() as total_count"));

    verifySchema(result, schema("total_count", "bigint"));
    verifyDataRows(result, rows(3L));
  }

  @Test
  public void testSpathWithComplexJsonArrayAccess() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | where isnotnull(`preferences{}`) | fields id,"
                    + " `preferences{}`"));

    verifySchema(result, schema("id", "bigint"), schema("preferences{}", "array"));
    verifyDataRows(result, rows(1L, arr("music", "travel")));
  }

  @Test
  public void testSpathWithNestedArray() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | where isnotnull(`nested{}.a`) | fields id, `nested{}.a`,"
                    + " `nested{}.arr{}`"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("nested{}.a", "array"),
        schema("nested{}.arr{}", "array"));
    verifyDataRows(result, rows(3L, arr("v1", "v2"), arr(1, 2, 3, 4, 5)));
  }

  @Test
  @Ignore("Join does not work now")
  public void testSpathWithJoin() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | join id test_dynamic_fields | fields id, `nested{}.a`,"
                    + " json_data"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("nested{}.a", "string"),
        schema("json_data", "string"));
    verifyDataRows(
        result,
        rows(1L, null, "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}"),
        rows(2L, null, "{\"name\": \"Jane\", \"age\": 25, \"country\": \"USA\"}"),
        rows(
            3L,
            "[\"v1\",\"v2\"]",
            "{\"product\": \"laptop\", \"price\": 999.99, \"brand\": \"Dell\"}"));
  }

  @Test
  @Ignore("Join does not work now")
  public void testSpathJoinWithSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | join id [index=test_dynamic_fields | spath input=json_data ] |"
                    + " fields id, `nested{}.a`, name, age"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("nested{}.a", "string"),
        schema("name", "string"),
        schema("age", "int"));
    // Current limitation: dynamic columns from left side won't be preserved after join
    verifyDataRows(
        result, rows(1L, null, "John", 30), rows(2L, null, "Jane", 25), rows(3L, null, null, null));
  }
}
