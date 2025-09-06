/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMultiValueStatsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
    loadIndex(Index.CALCS);
  }

  // ==================== Positive Tests - All Supported Data Types ====================

  @Test
  public void testListFunctionWithBoolean() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(boolean_value) as bool_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("bool_list", "array"));
    verifyDataRows(response, rows(List.of("true")));
  }

  @Test
  public void testListFunctionWithByte() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(byte_number) as byte_list", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("byte_list", "array"));
    verifyDataRows(response, rows(List.of("4")));
  }

  @Test
  public void testListFunctionWithShort() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(short_number) as short_list", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("short_list", "array"));
    verifyDataRows(response, rows(List.of("3")));
  }

  @Test
  public void testListFunctionWithInteger() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(integer_number) as int_list", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("int_list", "array"));
    verifyDataRows(response, rows(List.of("2")));
  }

  @Test
  public void testListFunctionWithLong() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(long_number) as long_list", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("long_list", "array"));
    verifyDataRows(response, rows(List.of("1")));
  }

  @Test
  public void testListFunctionWithFloat() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(float_number) as float_list", TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("float_list", "array"));
    verifyDataRows(response, rows(List.of("6.2")));
  }

  @Test
  public void testListFunctionWithDouble() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(double_number) as double_list",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(response, schema("double_list", "array"));
    verifyDataRows(response, rows(List.of("5.1")));
  }

  @Test
  public void testListFunctionWithString() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(keyword_value) as keyword_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("keyword_list", "array"));
    verifyDataRows(response, rows(List.of("keyword")));

    JSONObject textResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(text_value) as text_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(textResponse, schema("text_list", "array"));
    verifyDataRows(textResponse, rows(List.of("text")));
  }

  @Test
  public void testListFunctionWithDate() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(date_value) as date_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("date_list", "array"));
    // Date values should be returned as timestamp strings
    verifyDataRows(response, rows(List.of("2020-10-13 13:00:00")));
  }

  @Test
  public void testListFunctionWithTime() throws IOException {
    JSONObject response =
        executeQuery(withSource(TEST_INDEX_CALCS, "head 1 | stats list(time1) as time_list"));
    verifySchema(response, schema("time_list", "array"));
    // Time values are stored as strings in the test data
    verifyDataRows(response, rows(List.of("19:36:22")));
  }

  @Test
  public void testListFunctionWithTimestamp() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(date_nanos_value) as timestamp_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("timestamp_list", "array"));
    // Calcite converts timezone to UTC
    verifyDataRows(response, rows(List.of("2019-03-24 01:34:46.123456789")));
  }

  @Test
  public void testListFunctionWithIP() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(ip_value) as ip_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("ip_list", "array"));
    verifyDataRows(response, rows(List.of("127.0.0.1")));
  }

  @Test
  public void testListFunctionWithBinary() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(binary_value) as binary_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("binary_list", "array"));
    verifyDataRows(response, rows(List.of("U29tZSBiaW5hcnkgYmxvYg==")));
  }

  // ==================== Edge Cases and Complex Scenarios ====================

  @Test
  public void testListFunctionWithNullValues() throws IOException {
    JSONObject response =
        executeQuery(withSource(TEST_INDEX_CALCS, "head 5 | stats list(int0) as int_list"));
    verifySchema(response, schema("int_list", "array"));
    // Nulls are filtered out by list function
    verifyDataRows(response, rows(List.of("1", "7")));
  }

  @Test
  public void testListFunctionGroupBy() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats list(num0) as num_list by str0", TEST_INDEX_CALCS));
    verifySchema(response, schema("num_list", "array"), schema("str0", null, "string"));

    // Group by str0 field - should have different groups with their respective num0 values
    // Just verify we get some results with the correct schema
    assert response.has("datarows");
  }

  @Test
  public void testListFunctionMultipleFields() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | head 3 | stats list(str2) as str_list, list(int2) as int_list",
                TEST_INDEX_CALCS));
    verifySchema(response, schema("str_list", "array"), schema("int_list", "array"));

    // Verify we get arrays for both fields
    assert response.has("datarows");
    // Values should be collected from the first 3 rows (str2 and int2 columns)
    // The actual values depend on the test data - int2 column contains 5, -4, 5
    verifyDataRows(response, rows(List.of("one", "two", "three"), List.of("5", "-4", "5")));
  }

  @Test
  public void testListFunctionWithComplexGroupBy() throws IOException {
    // Test list aggregation with multiple grouping fields
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats list(num0) as values by str0, bool0",
                TEST_INDEX_CALCS));
    verifySchema(
        response,
        schema("values", "array"),
        schema("str0", null, "string"),
        schema("bool0", null, "boolean"));

    // Should have multiple groups based on str0 and bool0 combinations
    assert response.has("datarows");
    // Verify we get grouped results with proper values
    assert response.getJSONArray("datarows").length() > 0;
  }

  @Test
  public void testListFunctionEmptyResult() throws IOException {
    // Test list function with no matching records - simplify this test
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where str0 = 'NONEXISTENT' | stats list(num0) as empty_list",
                TEST_INDEX_CALCS));
    verifySchema(response, schema("empty_list", "array"));

    assert response.has("datarows");
    // When no records match, LIST returns null (not an empty list)
    verifyDataRows(response, rows((List<Object>) null));
  }

  // ==================== Advanced Functionality Tests ====================

  @Test
  public void testListFunctionWithObjectField() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(object_value.first) as object_field_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(response, schema("object_field_list", "array"));
    verifyDataRows(response, rows(List.of("Dale")));
  }

  @Test
  public void testListFunctionWithArithmeticExpression() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | head 3 | stats list(int3 + 1) as arithmetic_list", TEST_INDEX_CALCS));
    verifySchema(response, schema("arithmetic_list", "array"));
    verifyDataRows(response, rows(List.of("9", "14", "3")));
  }
}
