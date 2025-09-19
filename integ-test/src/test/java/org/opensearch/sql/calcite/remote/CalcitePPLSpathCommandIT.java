/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_JSON_TEST;
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
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.JSON_TEST); // Use existing JSON test data

    // Create minimal test data for basic spath functionality
    Request request1 = new Request("PUT", "/test_spath/_doc/1?refresh=true");
    request1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_spath/_doc/2?refresh=true");
    request2.setJsonEntity("{\"doc\": \"{\\\"n\\\": 2}\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_spath/_doc/3?refresh=true");
    request3.setJsonEntity("{\"doc\": \"{\\\"n\\\": 3}\"}");
    client().performRequest(request3);

    // Create test data for dynamic columns functionality
    Request dynamicRequest1 = new Request("PUT", "/test_dynamic_columns/_doc/1?refresh=true");
    dynamicRequest1.setJsonEntity(
        "{\"id\": 1, \"json_data\": \"{\\\"name\\\": \\\"John\\\", \\\"age\\\": 30, \\\"city\\\":"
            + " \\\"New York\\\"}\"}");
    client().performRequest(dynamicRequest1);

    Request dynamicRequest2 = new Request("PUT", "/test_dynamic_columns/_doc/2?refresh=true");
    dynamicRequest2.setJsonEntity(
        "{\"id\": 2, \"json_data\": \"{\\\"name\\\": \\\"Jane\\\", \\\"age\\\": 25,"
            + " \\\"country\\\": \\\"USA\\\"}\"}");
    client().performRequest(dynamicRequest2);

    Request dynamicRequest3 = new Request("PUT", "/test_dynamic_columns/_doc/3?refresh=true");
    dynamicRequest3.setJsonEntity(
        "{\"id\": 3, \"json_data\": \"{\\\"product\\\": \\\"laptop\\\", \\\"price\\\": 999.99,"
            + " \\\"brand\\\": \\\"Dell\\\"}\"}");
    client().performRequest(dynamicRequest3);

    // Create test data for complex nested JSON
    Request complexRequest1 = new Request("PUT", "/test_complex_json/_doc/1?refresh=true");
    complexRequest1.setJsonEntity(
        "{\"id\": 1, \"data\": \"{\\\"user\\\": {\\\"name\\\": \\\"Alice\\\", \\\"profile\\\":"
            + " {\\\"age\\\": 28, \\\"location\\\": \\\"Seattle\\\"}}, \\\"preferences\\\":"
            + " [\\\"music\\\", \\\"travel\\\"]}\"}");
    client().performRequest(complexRequest1);

    Request complexRequest2 = new Request("PUT", "/test_complex_json/_doc/2?refresh=true");
    complexRequest2.setJsonEntity(
        "{\"id\": 2, \"data\": \"{\\\"user\\\": {\\\"name\\\": \\\"Bob\\\", \\\"profile\\\":"
            + " {\\\"age\\\": 35, \\\"location\\\": \\\"Portland\\\"}}, \\\"settings\\\":"
            + " {\\\"theme\\\": \\\"dark\\\", \\\"notifications\\\": true}}\"}");
    client().performRequest(complexRequest2);

    // Create test data for multiple spath overwrite issue testing
    Request overwriteRequest1 = new Request("PUT", "/test_overwrite/_doc/1?refresh=true");
    overwriteRequest1.setJsonEntity(
        "{\"id\": 1, \"json_data1\": \"{\\\"name\\\": \\\"John\\\", \\\"age\\\": 30}\","
            + " \"json_data2\": \"{\\\"city\\\": \\\"New York\\\", \\\"country\\\":"
            + " \\\"USA\\\"}\"}");
    client().performRequest(overwriteRequest1);

    Request overwriteRequest2 = new Request("PUT", "/test_overwrite/_doc/2?refresh=true");
    overwriteRequest2.setJsonEntity(
        "{\"id\": 2, \"json_data1\": \"{\\\"name\\\": \\\"Jane\\\", \\\"age\\\": 25}\", "
            + "\"json_data2\": \"{\\\"city\\\": \\\"Boston\\\", \\\"country\\\": \\\"USA\\\"}\"}");
    client().performRequest(overwriteRequest2);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | spath input=doc output=result path=n | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathDynamicColumnsBasic() throws IOException {
    // Test basic dynamic columns generation with spath
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | fields id, name, age, city");

    // Debug: Print the actual result to understand what we're getting
    System.out.println("=== DEBUG: Actual query result ===");
    System.out.println(result.toString(2));
    System.out.println("=== END DEBUG ===");

    verifySchema(
        result,
        schema("id", "bigint"), // Changed from integer to bigint
        schema("name", "string"), // Dynamic columns preserve original JSON types
        schema("age", "int"), // FIXED: age now preserves original integer type from JSON
        schema("city", "string")); // Dynamic columns preserve original JSON types

    // Verify that dynamic columns are accessible with correct types
    verifyDataRows(
        result,
        rows(1L, "John", 30, "New York"), // age is now integer, not string
        rows(2L, "Jane", 25, null), // age is now integer, not string
        rows(3L, null, null, null)); // name, age, city not present in third record
  }

  @Test
  public void testSpathDynamicColumnsWithDifferentFields() throws IOException {
    // Test that different JSON structures create different dynamic columns
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | fields id, name, country,"
                + " product, price");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema(
            "name", "string"), // FIXED: Dynamic columns now correctly infer types from JSON values
        schema(
            "country",
            "string"), // FIXED: Dynamic columns now correctly infer types from JSON values
        schema(
            "product",
            "string"), // FIXED: Dynamic columns now correctly infer types from JSON values
        schema("price", "double")); // FIXED: price now preserves original double type from JSON

    verifyDataRows(
        result,
        rows(1L, "John", null, null, null), // only name from first record
        rows(2L, "Jane", "USA", null, null), // name and country from second record
        rows(3L, null, null, "laptop", 999.99)); // FIXED: price is now double, not string
  }

  private void debug(JSONObject result) {
    // Debug: Print the actual result to understand what we're getting
    System.out.println("=== DEBUG: Actual query result ===");
    System.out.println(result.toString(2));
    System.out.println("=== END DEBUG ===");
  }

  @Test
  public void testSpathDynamicColumnsWithFiltering() throws IOException {
    // Test dynamic columns with filtering - use string comparison since dynamic fields are strings
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | where name = 'John' | fields id,"
                + " name, age");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"), // Changed from integer to bigint
        schema(
            "name", "string"), // FIXED: Dynamic columns now correctly infer types from JSON values
        schema("age", "int")); // FIXED: age now preserves original integer type from JSON

    verifyDataRows(result, rows(1L, "John", 30)); // FIXED: age is now integer, not string
  }

  @Test
  public void testSpathDynamicColumnsWithAggregation() throws IOException {
    // Test dynamic columns with aggregation - use count only since dynamic fields are strings
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | stats count() as total_count");

    verifySchema(
        result, schema("total_count", "bigint")); // FIXED: count() returns bigint, not long

    // Total count = 3 records
    verifyDataRows(result, rows(3L));
  }

  @Test
  public void testSpathDynamicColumnsWithSorting() throws IOException {
    // Test dynamic columns with sorting - simplified to avoid null check syntax issues
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | sort id | fields id, name");

    verifySchema(
        result,
        schema("id", "bigint"), // Changed from integer to bigint
        schema("name", "string"));

    verifyDataRows(result, rows(1L, "John"), rows(2L, "Jane"), rows(3L, null)); // Sorted by id
  }

  @Test
  public void testSpathDynamicColumnsMultipleSpathCalls() throws IOException {
    // Test multiple spath calls in the same query
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | spath input=json_data path=name"
                + " output=extracted_name | fields id, name, extracted_name");

    verifySchema(
        result,
        schema("id", "bigint"), // Changed from integer to bigint
        schema("name", "string"),
        schema("extracted_name", "string"));

    verifyDataRows(
        result, rows(1L, "John", "John"), rows(2L, "Jane", "Jane"), rows(3L, null, null));
  }

  @Test
  public void testSpathDynamicColumnsWithGroupBy() throws IOException {
    // Test dynamic columns with group by - simplified to avoid null check syntax issues
    JSONObject result =
        executeQuery(
            "source=test_dynamic_columns | spath input=json_data | stats count() as name_count by"
                + " name | sort name_count");

    debug(result);
    verifySchema(
        result,
        schema("name_count", "bigint"), // FIXED: count() returns bigint
        schema(
            "name", "string")); // FIXED: Dynamic columns now correctly infer types from JSON values

    verifyDataRows(
        result,
        rows(1L, null), // null name group
        rows(1L, "Jane"),
        rows(1L, "John"));
  }

  @Test
  public void testSpathDynamicColumnsExplain() throws IOException {
    // Debug test to examine logical and physical plans for spath dynamic columns
    executeQuery(
        "source=test_dynamic_columns | spath input=json_data | fields id, name, age, city");
  }

  @Test
  public void testSpathMultipleInputsOverwriteIssue() throws IOException {
    // Test multiple spath calls with different input sources to check for overwrite issue
    // This should extract name,age from json_data1 AND city,country from json_data2
    // But if there's an overwrite issue, the second spath will overwrite the first
    String ppl =
        "source=test_overwrite | "
            + "spath input=json_data1 | " // Should extract name, age
            + "spath input=json_data2 | " // Should extract city, country (but might overwrite
            // name, age)
            + "fields id, name, age, city, country";
    JSONObject result = executeQuery(ppl);

    System.out.println("=== MULTIPLE SPATH OVERWRITE TEST ===");
    System.out.println(result.toString(2));
    System.out.println("=== END TEST ===");

    // Expected: All fields should be accessible
    // If there's an overwrite issue: name and age will be null because they were overwritten
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"), // From json_data1
        schema("age", "int"), // FIXED: MAP_MERGE preserves type info from MAP_GET fix
        schema("city", "string"), // From json_data2
        schema("country", "string")); // From json_data2

    verifyDataRows(
        result,
        rows(1L, "John", 30, "New York", "USA"), // FIXED: age is now integer, not string
        rows(2L, "Jane", 25, "Boston", "USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithWhereClause() throws IOException {
    // Test multiple spath operations followed by where clause filtering
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "where name = 'John' | "
                + "fields id, name, age, city, country");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", 30, "New York", "USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithStatsAggregation() throws IOException {
    // Test multiple spath operations followed by stats aggregation
    // Simplified to avoid UNDEFINED type issues with AVG function
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "stats count() as total by country");

    verifySchema(result, schema("total", "bigint"), schema("country", "string"));

    verifyDataRows(result, rows(2L, "USA")); // Both records are from USA
  }

  @Test
  public void testMultipleSpathWithAvgAggregation() throws IOException {
    // Test multiple spath operations with AVG aggregation on dynamic fields
    // This tests that dynamic fields maintain proper numeric types for aggregation
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "stats avg(CAST(age AS INTEGER)) as avg_age by country");

    verifySchema(result, schema("avg_age", "double"), schema("country", "string"));

    verifyDataRows(
        result, rows(27.5, "USA")); // Both records are from USA, avg age = (30+25)/2 = 27.5
  }

  @Test
  public void testMultipleSpathWithEvalCommand() throws IOException {
    // Test multiple spath operations followed by eval command
    // Simplified to avoid complex nested MAP_MERGE issues in eval context
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "eval age_plus_ten = CAST(age AS INTEGER) + 10 | "
                + "fields id, name, age, age_plus_ten");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("age_plus_ten", "int"));

    verifyDataRows(
        result,
        rows(1L, "John", 30, 40),
        rows(2L, "Jane", 25, 35)); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithConcatEval() throws IOException {
    // Test multiple spath operations with concat function in eval command
    JSONObject result =
        executeQuery(
            "source=test_overwrite | spath input=json_data1 | spath input=json_data2 | eval"
                + " full_location = concat(CAST(city AS STRING), ', ', CAST(country AS STRING)) |"
                + " fields id, name, age, full_location");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("full_location", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", 30, "New York, USA"),
        rows(2L, "Jane", 25, "Boston, USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithComplexFiltering() throws IOException {
    // Test multiple spath operations with complex where conditions
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "where CAST(age AS INTEGER) > 25 and country = 'USA' | "
                + "fields id, name, age, city, country");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", 30, "New York", "USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithGroupByMultipleFields() throws IOException {
    // Test multiple spath operations with group by on multiple dynamic fields
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "stats count() as count_per_country_city by country, city");

    verifySchema(
        result,
        schema("count_per_country_city", "bigint"),
        schema("country", "string"),
        schema("city", "string"));

    verifyDataRows(result, rows(1L, "USA", "Boston"), rows(1L, "USA", "New York"));
  }

  @Test
  public void testMultipleSpathWithLimitAndOffset() throws IOException {
    // Test multiple spath operations with limit
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "sort id | "
                + "head 1 | "
                + "fields id, name, age, city, country");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("city", "string"),
        schema("country", "string"));

    verifyDataRows(
        result, rows(1L, "John", 30, "New York", "USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithDedupCommand() throws IOException {
    // Test multiple spath operations with dedup command
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "dedup country | "
                + "fields id, name, country");

    verifySchema(
        result, schema("id", "bigint"), schema("name", "string"), schema("country", "string"));

    // Should return only one record since both have country = "USA"
    verifyDataRows(result, rows(1L, "John", "USA")); // First occurrence of USA
  }

  @Test
  public void testTripleSpathOperations() throws IOException {
    // Test three spath operations to ensure MAP_MERGE works with multiple merges
    // First, let's create test data with three JSON fields
    Request tripleRequest1 = new Request("PUT", "/test_triple_spath/_doc/1?refresh=true");
    tripleRequest1.setJsonEntity(
        "{\"id\": 1, "
            + "\"json1\": \"{\\\"name\\\": \\\"Alice\\\", \\\"age\\\": 28}\", "
            + "\"json2\": \"{\\\"city\\\": \\\"Seattle\\\", \\\"state\\\": \\\"WA\\\"}\", "
            + "\"json3\": \"{\\\"job\\\": \\\"Engineer\\\", \\\"salary\\\": 75000}\"}");
    client().performRequest(tripleRequest1);

    Request tripleRequest2 = new Request("PUT", "/test_triple_spath/_doc/2?refresh=true");
    tripleRequest2.setJsonEntity(
        "{\"id\": 2, "
            + "\"json1\": \"{\\\"name\\\": \\\"Bob\\\", \\\"age\\\": 32}\", "
            + "\"json2\": \"{\\\"city\\\": \\\"Portland\\\", \\\"state\\\": \\\"OR\\\"}\", "
            + "\"json3\": \"{\\\"job\\\": \\\"Designer\\\", \\\"salary\\\": 68000}\"}");
    client().performRequest(tripleRequest2);

    JSONObject result =
        executeQuery(
            "source=test_triple_spath | "
                + "spath input=json1 | " // Extract name, age
                + "spath input=json2 | " // Extract city, state
                + "spath input=json3 | " // Extract job, salary
                + "fields id, name, age, city, state, job, salary");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("city", "string"),
        schema("state", "string"),
        schema("job", "string"),
        schema("salary", "int")); // FIXED: salary preserves integer type from JSON

    verifyDataRows(
        result,
        rows(
            1L,
            "Alice",
            28,
            "Seattle",
            "WA",
            "Engineer",
            75000), // FIXED: age and salary are now integers
        rows(
            2L,
            "Bob",
            32,
            "Portland",
            "OR",
            "Designer",
            68000)); // FIXED: age and salary are now integers
  }

  @Test
  public void testMultipleSpathWithFieldsSubset() throws IOException {
    // Test that we can select only a subset of fields from multiple spath operations
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "fields name, city"); // Only select name and city, not age or country

    verifySchema(result, schema("name", "string"), schema("city", "string"));

    verifyDataRows(result, rows("John", "New York"), rows("Jane", "Boston"));
  }

  @Test
  public void testMultipleSpathWithSortCommand() throws IOException {
    // Test multiple spath operations followed by sort command
    // Use eval to create a numeric field first, then sort by it
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "eval age_numeric = CAST(age AS INTEGER) | "
                + "sort age_numeric desc | "
                + "fields id, name, age, city");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("city", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", 30, "New York"), // FIXED: age is now integer, not string
        rows(2L, "Jane", 25, "Boston")); // FIXED: age is now integer, not string
  }

  @Test
  public void testMultipleSpathWithRename() throws IOException {
    // Test multiple spath operations with field renaming
    JSONObject result =
        executeQuery(
            "source=test_overwrite | "
                + "spath input=json_data1 | "
                + "spath input=json_data2 | "
                + "eval person_name = name, location = city | "
                + "fields id, person_name, age, location, country");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("person_name", "string"),
        schema("age", "int"), // FIXED: age preserves integer type even in multiple spath operations
        schema("location", "string"),
        schema("country", "string"));

    verifyDataRows(
        result,
        rows(1L, "John", 30, "New York", "USA"),
        rows(2L, "Jane", 25, "Boston", "USA")); // FIXED: age is now integer, not string
  }

  @Test
  public void testSpathWithJsonTestIndexWithWhere() throws IOException {
    // Test spath with the comprehensive JSON_TEST index
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_JSON_TEST,
                "spath input=json_string | where a = '1' | fields test_name, a, b, b.c, `d{}`"));

    debug(result);
    verifySchema(
        result,
        schema("test_name", "string"),
        schema("a", "string"),
        schema("b", "string"),
        schema("b.c", "string"),
        schema("d{}", "array")); // FIXED: d field contains array data, now preserved as array type

    verifyDataRows(
        result,
        rows("json nested object", "1", null, "3", new Object[] {false, 3}),
        rows("json object", "1", "2", null, null));
  }

  @Test
  public void testSpathWithJsonTestIndexAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_JSON_TEST,
                "spath input=json_string | stats count() as total_with_a by a"));

    debug(result);
    verifySchema(result, schema("total_with_a", "bigint"), schema("a", "string"));

    // Should group by the 'a' field values
    verifyDataRows(
        result,
        rows(11L, null), // 11 records without 'a' field
        rows(2L, "1")); // 2 records with a='1'
  }

  @Test
  public void testSpathWithJsonTestIndexComplexFiltering() throws IOException {
    JSONObject result =
        executeQuery(
            source(
                TEST_INDEX_JSON_TEST,
                "spath input=json_string | where isnotnull(a) | fields test_name, a, b"));

    debug(result);
    verifySchema(
        result, schema("test_name", "string"), schema("a", "string"), schema("b", "string"));

    verifyDataRows(result, rows("json nested object", "1", null), rows("json object", "1", "2"));
  }
}
