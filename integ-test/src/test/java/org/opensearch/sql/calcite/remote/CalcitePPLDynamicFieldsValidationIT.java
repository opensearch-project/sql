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

/**
 * Comprehensive integration tests to validate dynamic field handling across PPL commands. Uses
 * spath to create dynamic columns and tests field references in various contexts.
 *
 * <p>This test suite validates core dynamic field functionality that is production-ready.
 */
public class CalcitePPLDynamicFieldsValidationIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Create test data for dynamic field validation
    setupDynamicFieldTestData();
  }

  private void setupDynamicFieldTestData() throws IOException {
    // Test data for basic dynamic field operations
    Request request1 = new Request("PUT", "/test_dynamic_fields/_doc/1?refresh=true");
    request1.setJsonEntity(
        "{\"id\": 1, \"category\": \"electronics\", "
            + "\"details\": \"{\\\"name\\\": \\\"laptop\\\", \\\"price\\\": 999.99, "
            + "\\\"brand\\\": \\\"Dell\\\", \\\"rating\\\": 4.5, \\\"in_stock\\\": true, "
            + "\\\"quantity\\\": 10}\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_dynamic_fields/_doc/2?refresh=true");
    request2.setJsonEntity(
        "{\"id\": 2, \"category\": \"electronics\", "
            + "\"details\": \"{\\\"name\\\": \\\"smartphone\\\", \\\"price\\\": 699.99, "
            + "\\\"brand\\\": \\\"Apple\\\", \\\"rating\\\": 4.8, \\\"in_stock\\\": false, "
            + "\\\"quantity\\\": 0}\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_dynamic_fields/_doc/3?refresh=true");
    request3.setJsonEntity(
        "{\"id\": 3, \"category\": \"books\", "
            + "\"details\": \"{\\\"name\\\": \\\"Java Guide\\\", \\\"price\\\": 49.99, "
            + "\\\"author\\\": \\\"John Doe\\\", \\\"rating\\\": 4.2, \\\"in_stock\\\": true, "
            + "\\\"pages\\\": 350}\"}");
    client().performRequest(request3);

    Request request4 = new Request("PUT", "/test_dynamic_fields/_doc/4?refresh=true");
    request4.setJsonEntity(
        "{\"id\": 4, \"category\": \"books\", "
            + "\"details\": \"{\\\"name\\\": \\\"Python Basics\\\", \\\"price\\\": 39.99, "
            + "\\\"author\\\": \\\"Jane Smith\\\", \\\"rating\\\": 4.0, \\\"in_stock\\\": true, "
            + "\\\"pages\\\": 280}\"}");
    client().performRequest(request4);

    Request request5 = new Request("PUT", "/test_dynamic_fields/_doc/5?refresh=true");
    request5.setJsonEntity(
        "{\"id\": 5, \"category\": \"electronics\", "
            + "\"details\": \"{\\\"name\\\": \\\"tablet\\\", \\\"price\\\": 299.99, "
            + "\\\"brand\\\": \\\"Samsung\\\", \\\"rating\\\": 4.3, \\\"in_stock\\\": true, "
            + "\\\"quantity\\\": 25}\"}");
    client().performRequest(request5);
  }

  private void debug(JSONObject result) {
    System.out.println("=== DEBUG: Query Result ===");
    System.out.println(result.toString(2));
    System.out.println("=== END DEBUG ===");
  }

  // HIGH PRIORITY TESTS - Core field operations that are confirmed working

  @Test
  public void testDynamicFieldsInFilterConditions() throws IOException {
    // Test dynamic fields in WHERE clauses - HIGH PRIORITY validation
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where name = 'laptop' | fields id, name, price, brand");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price", "double"),
        schema("brand", "string"));

    verifyDataRows(result, rows(1L, "laptop", 999.99, "Dell"));
  }

  @Test
  public void testDynamicFieldsInComplexFilterConditions() throws IOException {
    // Test complex filter conditions with dynamic fields
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where CAST(price AS DOUBLE) > 500.0 and in_stock = true | "
                + "fields id, name, price, in_stock");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price", "double"),
        schema("in_stock", "boolean"));

    verifyDataRows(result, rows(1L, "laptop", 999.99, true));
  }

  @Test
  public void testDynamicFieldsInSortOperations() throws IOException {
    // Test dynamic fields in ORDER BY clauses - HIGH PRIORITY validation
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "sort price desc | fields id, name, price");

    debug(result);
    verifySchema(
        result, schema("id", "bigint"), schema("name", "string"), schema("price", "double"));

    verifyDataRows(
        result,
        rows(1L, "laptop", 999.99),
        rows(2L, "smartphone", 699.99),
        rows(5L, "tablet", 299.99),
        rows(3L, "Java Guide", 49.99),
        rows(4L, "Python Basics", 39.99));
  }

  @Test
  public void testDynamicFieldsInSimpleAggregation() throws IOException {
    // Test dynamic fields in simple aggregation
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "stats count() as total_count, avg(CAST(price AS DOUBLE)) as avg_price");

    debug(result);
    verifySchema(result, schema("total_count", "bigint"), schema("avg_price", "double"));

    verifyDataRows(result, rows(5L, 417.98999999999995)); // Adjusted for floating point precision
  }

  @Test
  public void testDynamicFieldsInGroupByWithCategory() throws IOException {
    // Test dynamic fields in GROUP BY with existing field
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "stats avg(CAST(price AS DOUBLE)) as avg_price by category | "
                + "sort category");

    debug(result);
    verifySchema(result, schema("avg_price", "double"), schema("category", "string"));

    verifyDataRows(
        result,
        rows(44.99, "books"),
        rows(666.6566666666666, "electronics")); // Adjusted for floating point precision
  }

  @Test
  public void testDynamicFieldsInGroupByWithDynamicField() throws IOException {
    // Test dynamic fields in GROUP BY with dynamic field - filter to avoid nulls
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where category = 'electronics' | "
                + "stats count() as brand_count by brand | "
                + "sort brand");

    debug(result);
    verifySchema(result, schema("brand_count", "bigint"), schema("brand", "string"));

    verifyDataRows(result, rows(1L, "Apple"), rows(1L, "Dell"), rows(1L, "Samsung"));
  }

  @Test
  public void testDynamicFieldsWithEvalExpressions() throws IOException {
    // Test dynamic fields in EVAL expressions
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "eval price_doubled = CAST(price AS DOUBLE) * 2 | "
                + "fields id, name, price, price_doubled | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price", "double"),
        schema("price_doubled", "double"));

    verifyDataRows(
        result,
        rows(1L, "laptop", 999.99, 1999.98),
        rows(2L, "smartphone", 699.99, 1399.98),
        rows(3L, "Java Guide", 49.99, 99.98),
        rows(4L, "Python Basics", 39.99, 79.98),
        rows(5L, "tablet", 299.99, 599.98));
  }

  @Test
  public void testDynamicFieldsWithCaseExpressions() throws IOException {
    // Test dynamic fields in CASE expressions
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | eval price_category ="
                + " case(CAST(price AS DOUBLE) > 500, 'expensive' else 'affordable') | fields id,"
                + " name, price, price_category | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price", "double"),
        schema("price_category", "string"));

    verifyDataRows(
        result,
        rows(1L, "laptop", 999.99, "expensive"),
        rows(2L, "smartphone", 699.99, "expensive"),
        rows(3L, "Java Guide", 49.99, "affordable"),
        rows(4L, "Python Basics", 39.99, "affordable"),
        rows(5L, "tablet", 299.99, "affordable"));
  }

  @Test
  public void testDynamicFieldsTypePreservation() throws IOException {
    // Test that dynamic field types are preserved correctly
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where category = 'electronics' | "
                + "fields id, name, price, rating, in_stock, quantity | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price", "double"),
        schema("rating", "double"),
        schema("in_stock", "boolean"),
        schema("quantity", "int"));

    verifyDataRows(
        result,
        rows(1L, "laptop", 999.99, 4.5, true, 10),
        rows(2L, "smartphone", 699.99, 4.8, false, 0),
        rows(5L, "tablet", 299.99, 4.3, true, 25));
  }

  @Test
  public void testDynamicFieldsWithMultipleOperations() throws IOException {
    // Test dynamic fields across multiple operations in sequence
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where CAST(price AS DOUBLE) > 100 | "
                + "eval discounted_price = CAST(price AS DOUBLE) * 0.9 | "
                + "sort discounted_price desc | "
                + "fields name, price, discounted_price");

    debug(result);
    verifySchema(
        result,
        schema("name", "string"),
        schema("price", "double"),
        schema("discounted_price", "double"));

    verifyDataRows(
        result,
        rows("laptop", 999.99, 899.991),
        rows("smartphone", 699.99, 629.991),
        rows("tablet", 299.99, 269.99100000000004)); // Adjusted for floating point precision
  }

  @Test
  public void testDynamicFieldsWithAggregationAndFiltering() throws IOException {
    // Test dynamic fields with aggregation and filtering combined
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | where in_stock = true | stats"
                + " avg(CAST(price AS DOUBLE)) as avg_price, count() as count_items by category |"
                + " sort category");

    debug(result);
    verifySchema(
        result,
        schema("avg_price", "double"),
        schema("count_items", "bigint"),
        schema("category", "string"));

    verifyDataRows(
        result,
        rows(44.99, 2L, "books"),
        rows(649.99, 2L, "electronics")); // laptop and tablet are in stock
  }

  @Test
  public void testDynamicFieldsWithStringOperations() throws IOException {
    // Test dynamic fields with string operations
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where category = 'books' | "
                + "eval name_length = length(name) | "
                + "fields id, name, author, name_length | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("author", "string"),
        schema("name_length", "int"));

    verifyDataRows(
        result,
        rows(3L, "Java Guide", "John Doe", 10),
        rows(4L, "Python Basics", "Jane Smith", 13));
  }

  @Test
  public void testDynamicFieldsWithNumericOperations() throws IOException {
    // Test dynamic fields with numeric operations - simplified to avoid type issues
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where category = 'electronics' and quantity != null | "
                + "eval price_per_unit = CAST(price AS DOUBLE) / CAST(quantity AS INTEGER) | "
                + "fields id, quantity, price, price_per_unit | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("quantity", "int"),
        schema("price", "double"),
        schema("price_per_unit", "double"));

    verifyDataRows(result, rows(1L, 10, 999.99, 99.999), rows(5L, 25, 299.99, 11.9996));
  }

  @Test
  public void testDynamicFieldsWithBooleanOperations() throws IOException {
    // Test dynamic fields with boolean operations
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "eval is_available = in_stock = true, "
                + "is_premium = CAST(price AS DOUBLE) > 500 | "
                + "fields id, name, in_stock, is_available, is_premium | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("in_stock", "boolean"),
        schema("is_available", "boolean"),
        schema("is_premium", "boolean"));

    verifyDataRows(
        result,
        rows(1L, "laptop", true, true, true),
        rows(2L, "smartphone", false, false, true),
        rows(3L, "Java Guide", true, true, false),
        rows(4L, "Python Basics", true, true, false),
        rows(5L, "tablet", true, true, false));
  }

  @Test
  public void testDynamicFieldsWithComplexAggregation() throws IOException {
    // Test dynamic fields in complex aggregation scenarios
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "stats avg(CAST(price AS DOUBLE)) as avg_price, "
                + "count() as item_count, "
                + "max(CAST(rating AS DOUBLE)) as max_rating by category | "
                + "sort category");

    debug(result);
    verifySchema(
        result,
        schema("avg_price", "double"),
        schema("item_count", "bigint"),
        schema("max_rating", "double"),
        schema("category", "string"));

    verifyDataRows(
        result,
        rows(44.99, 2L, 4.2, "books"),
        rows(666.6566666666666, 3L, 4.8, "electronics")); // Adjusted for floating point precision
  }

  @Test
  public void testDynamicFieldsWithInOperator() throws IOException {
    // Test dynamic fields with IN operator
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where name in ('laptop', 'smartphone', 'tablet') | "
                + "fields id, name, brand, price | sort price desc");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("brand", "string"),
        schema("price", "double"));

    verifyDataRows(
        result,
        rows(1L, "laptop", "Dell", 999.99),
        rows(2L, "smartphone", "Apple", 699.99),
        rows(5L, "tablet", "Samsung", 299.99));
  }

  @Test
  public void testDynamicFieldsWithRangeFiltering() throws IOException {
    // Test dynamic fields with range filtering
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where CAST(price AS DOUBLE) between 100 and 800 | "
                + "fields id, name, price | sort price desc");

    debug(result);
    verifySchema(
        result, schema("id", "bigint"), schema("name", "string"), schema("price", "double"));

    verifyDataRows(result, rows(2L, "smartphone", 699.99), rows(5L, "tablet", 299.99));
  }

  @Test
  public void testDynamicFieldsWithMultipleEvalOperations() throws IOException {
    // Test multiple eval operations with dynamic fields
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "where category = 'electronics' | "
                + "eval price_doubled = CAST(price AS DOUBLE) * 2, "
                + "rating_percentage = CAST(rating AS DOUBLE) * 20, "
                + "total_value = CAST(price AS DOUBLE) * CAST(quantity AS INTEGER) | "
                + "fields id, name, price_doubled, rating_percentage, total_value | sort id");

    debug(result);
    verifySchema(
        result,
        schema("id", "bigint"),
        schema("name", "string"),
        schema("price_doubled", "double"),
        schema("rating_percentage", "double"),
        schema("total_value", "double"));

    verifyDataRows(
        result,
        rows(1L, "laptop", 1999.98, 90.0, 9999.9),
        rows(2L, "smartphone", 1399.98, 96.0, 0.0),
        rows(5L, "tablet", 599.98, 86.0, 7499.75));
  }

  @Test
  public void testDynamicFieldsValidationSummary() throws IOException {
    // Summary test that validates key dynamic field functionality
    JSONObject result =
        executeQuery(
            "source=test_dynamic_fields | spath input=details | "
                + "stats avg(CAST(price AS DOUBLE)) as avg_price, "
                + "count() as total_items by category | "
                + "sort category");

    debug(result);
    verifySchema(
        result,
        schema("avg_price", "double"),
        schema("total_items", "bigint"),
        schema("category", "string"));

    verifyDataRows(
        result,
        rows(44.99, 2L, "books"),
        rows(666.6566666666666, 3L, "electronics")); // Adjusted for floating point precision
  }
}
