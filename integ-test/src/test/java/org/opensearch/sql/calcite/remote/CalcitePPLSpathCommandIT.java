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
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC_COLUMNS = "test_dynamic_columns";
  private static final String TEST_INDEX_COMPLEX_JSON = "test_complex_json";
  private static final String TEST_INDEX_OVERWRITE = "test_overwrite";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.JSON_TEST);

    createDynamicColumnsTestData();
    createComplexJsonTestData();
    createMultipleSpathTestData();
  }

  private void createDynamicColumnsTestData() throws IOException {
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS, 1, "{\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS, 2, "{\"name\": \"Jane\", \"age\": 25, \"country\": \"USA\"}");
    createDocumentWithIdAndJsonData(
        TEST_INDEX_DYNAMIC_COLUMNS,
        3,
        "{\"product\": \"laptop\", \"price\": 999.99, \"brand\": \"Dell\"}");
  }

  private void createComplexJsonTestData() throws IOException {
    createDocumentWithIdAndJsonField(
        TEST_INDEX_COMPLEX_JSON,
        1,
        "data",
        "{\"user\": {\"name\": \"Alice\", \"profile\": {\"age\": 28, \"location\": \"Seattle\"}},"
            + " \"preferences\": [\"music\", \"travel\"]}");
    createDocumentWithIdAndJsonField(
        TEST_INDEX_COMPLEX_JSON,
        2,
        "data",
        "{\"user\": {\"name\": \"Bob\", \"profile\": {\"age\": 35, \"location\": \"Portland\"}},"
            + " \"settings\": {\"theme\": \"dark\", \"notifications\": true}}");
    createDocumentWithIdAndJsonField(
        TEST_INDEX_COMPLEX_JSON,
        3,
        "data",
        "{\"user\": {\"name\": \"John\", \"profile\": {\"age\": 40, \"location\": \"Kirkland\"}},"
            + " \"nested\": [{\"a\": \"v1\", \"arr\": [1, 2, 3]}, {\"a\": \"v2\", \"arr\": [4,"
            + " 5]}]}");
  }

  private void createMultipleSpathTestData() throws IOException {
    createDocumentWithMultipleJsonFields(
        TEST_INDEX_OVERWRITE,
        1,
        "json_data1",
        "{\"name\": \"John\", \"age\": 30}",
        "json_data2",
        "{\"city\": \"New York\", \"country\": \"USA\"}");
    createDocumentWithMultipleJsonFields(
        TEST_INDEX_OVERWRITE,
        2,
        "json_data1",
        "{\"name\": \"Jane\", \"age\": 25}",
        "json_data2",
        "{\"city\": \"Boston\", \"country\": \"USA\"}");
  }

  private void createDocumentWithIdAndJsonData(String index, int id, String jsonContent)
      throws IOException {
    createDocumentWithIdAndJsonField(index, id, "json_data", jsonContent);
  }

  private void createDocumentWithMultipleJsonFields(
      String index,
      int id,
      String field1Name,
      String json1Content,
      String field2Name,
      String json2Content)
      throws IOException {
    Request request = new Request("PUT", String.format("/%s/_doc/%d?refresh=true", index, id));
    String escapedJson1 = json1Content.replace("\"", "\\\"");
    String escapedJson2 = json2Content.replace("\"", "\\\"");
    request.setJsonEntity(
        String.format(
            "{\"id\": %d, \"%s\": \"%s\", \"%s\": \"%s\"}",
            id, field1Name, escapedJson1, field2Name, escapedJson2));
    client().performRequest(request);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data output=result path=name | fields id, result"));
    verifySchema(result, schema("id", "bigint"), schema("result", "string"));
    verifyDataRows(result, rows(1L, "John"), rows(2L, "Jane"), rows(3L, null));
  }

  @Test
  public void testSpathDynamicColumnsBasic() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS, "spath input=json_data | fields id, name, age, city"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", "30", "New York"),
        rows(2L, "Jane", "25", null),
        rows(3L, null, null, null));
  }

  @Test
  public void testSpathDynamicColumnsWithDifferentFields() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | fields id, name, country, product, price"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("country", "string"),
        schema("product", "string"),
        schema("price", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", null, null, null),
        rows(2L, "Jane", "USA", null, null),
        rows(3L, null, null, "laptop", "999.99"));
  }

  @Test
  public void testSpathDynamicColumnsWithFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | where name = 'John' | fields id, name, age"));

    verifySchema(result, schema("id", "bigint"), schema("name", "string"), schema("age", "string"));
    verifyDataRows(result, rows(1L, "John", "30"));
  }

  @Test
  public void testSpathDynamicColumnsWithAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | stats count() as total_count"));

    verifySchema(result, schema("total_count", "bigint"));
    verifyDataRows(result, rows(3L));
  }

  @Test
  public void testSpathDynamicColumnsWithSorting() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS, "spath input=json_data | sort id | fields id, name"));

    verifySchema(result, schema("id", "bigint"), schema("name", "string"));
    verifyDataRows(result, rows(1L, "John"), rows(2L, "Jane"), rows(3L, null));
  }

  @Test
  public void testSpathDynamicColumnsMultipleSpathCalls() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | spath input=json_data path=name output=extracted_name |"
                    + " fields id, name, extracted_name"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("extracted_name", "string"));

    verifyDataRows(
        result, rows(1L, "John", "John"), rows(2L, "Jane", "Jane"), rows(3L, null, null));
  }

  @Test
  public void testSpathDynamicColumnsWithGroupBy() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_DYNAMIC_COLUMNS,
                "spath input=json_data | stats count() as name_count by name | sort name_count"));

    verifySchema(result, schema("name_count", "bigint"), schema("name", "string"));
    verifyDataRows(result, rows(1L, null), rows(1L, "Jane"), rows(1L, "John"));
  }

  @Test
  public void testSpathDynamicColumnsExplain() throws IOException {
    executeQuery(
        source(TEST_INDEX_DYNAMIC_COLUMNS, "spath input=json_data | fields id, name, age, city"));
  }

  @Test
  public void testSpathMultipleInputsOverwriteIssue() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | fields id, name, age, city,"
                    + " country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", "30", "New York", "USA"), rows(2L, "Jane", "25", "Boston", "USA"));
  }

  @Test
  public void testMultipleSpathWithWhereClause() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | where name = 'John' | fields id,"
                    + " name, age, city, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(result, rows(1L, "John", "30", "New York", "USA"));
  }

  @Test
  public void testMultipleSpathWithStatsAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | stats count() as total by"
                    + " country"));

    verifySchema(result, schema("total", "bigint"), schema("country", "string"));
    verifyDataRows(result, rows(2L, "USA"));
  }

  @Test
  public void testMultipleSpathWithAvgAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | stats avg(CAST(age AS INTEGER))"
                    + " as avg_age by country"));

    verifySchema(result, schema("avg_age", "double"), schema("country", "string"));
    verifyDataRows(result, rows(27.5, "USA"));
  }

  @Test
  public void testMultipleSpathWithEvalCommand() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | eval age_plus_ten = CAST(age AS"
                    + " INTEGER) + 10 | fields id, name, age, age_plus_ten"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("age_plus_ten", "int"));
    verifyDataRows(result, rows(1L, "John", "30", 40), rows(2L, "Jane", "25", 35));
  }

  @Test
  public void testMultipleSpathWithConcatEval() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | eval full_location ="
                    + " concat(CAST(city AS STRING), ', ', CAST(country AS STRING)) | fields id,"
                    + " name, age, full_location"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("full_location", "string"));
    verifyDataRows(
        result, rows(1L, "John", "30", "New York, USA"), rows(2L, "Jane", "25", "Boston, USA"));
  }

  @Test
  public void testMultipleSpathWithComplexFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | where CAST(age AS INTEGER) > 25"
                    + " and country = 'USA' | fields id, name, age, city, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));
    verifyDataRows(result, rows(1L, "John", "30", "New York", "USA"));
  }

  @Test
  public void testMultipleSpathWithGroupByMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | stats count() as"
                    + " count_per_country_city by country, city"));

    verifySchema(
        result,
        schema("count_per_country_city", "bigint"),
        schema("country", "string"),
        schema("city", "string"));
    verifyDataRows(result, rows(1L, "USA", "Boston"), rows(1L, "USA", "New York"));
  }

  @Test
  public void testMultipleSpathWithLimitAndOffset() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | sort id | head 1 | fields id,"
                    + " name, age, city, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"),
        schema("country", "string"));
    verifyDataRows(result, rows(1L, "John", "30", "New York", "USA"));
  }

  @Test
  public void testMultipleSpathWithDedupCommand() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | dedup country | fields id, name,"
                    + " country"));

    verifySchema(
        result, schema("id", "bigint"), schema("name", "string"), schema("country", "string"));
    verifyDataRows(result, rows(1L, "John", "USA"));
  }

  @Test
  public void testMultipleSpathWithFieldsSubset() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | fields name, city"));

    verifySchema(result, schema("name", "string"), schema("city", "string"));
    verifyDataRows(result, rows("John", "New York"), rows("Jane", "Boston"));
  }

  @Test
  public void testMultipleSpathWithSortCommand() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | eval age_numeric = CAST(age AS"
                    + " INTEGER) | sort age_numeric desc | fields id, name, age, city"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "string"),
        schema("city", "string"));
    verifyDataRows(result, rows(1L, "John", "30", "New York"), rows(2L, "Jane", "25", "Boston"));
  }

  @Test
  public void testMultipleSpathWithRename() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_OVERWRITE,
                "spath input=json_data1 | spath input=json_data2 | eval person_name = name,"
                    + " location = city | fields id, person_name, age, location, country"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("person_name", "string"),
        schema("age", "string"),
        schema("location", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", "30", "New York", "USA"), rows(2L, "Jane", "25", "Boston", "USA"));
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
        schema("user.profile.age", "string"),
        schema("user.profile.location", "string"));
    verifyDataRows(
        result,
        rows(1L, "Alice", "28", "Seattle"),
        rows(2L, "Bob", "35", "Portland"),
        rows(3L, "John", "40", "Kirkland"));
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

    verifySchema(result, schema("id", "bigint"), schema("preferences{}", "string"));
    verifyDataRows(result, rows(1L, "[\"music\",\"travel\"]"));
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
        schema("nested{}.a", "string"),
        schema("nested{}.arr{}", "string"));
    verifyDataRows(result, rows(3L, "[\"v1\",\"v2\"]", "[\"1\",\"2\",\"3\",\"4\",\"5\"]"));
  }

  @Test
  public void testSpathWithJoin() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | join id test_dynamic_columns | fields id, `nested{}.a`,"
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
  public void testSpathJoinWithSpath() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_COMPLEX_JSON,
                "spath input=data | join id [index=test_dynamic_columns | spath input=json_data ] |"
                    + " fields id, `nested{}.a`, name, age"));

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("nested{}.a", "string"),
        schema("name", "string"),
        schema("age", "string"));
    // Current limitation: dynamic columns from left side won't be preserved after join
    verifyDataRows(
        result,
        rows(1L, null, "John", "30"),
        rows(2L, null, "Jane", "25"),
        rows(3L, null, null, null));
  }
}
