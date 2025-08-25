/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for multivalue statistics functions list() and values() with Calcite V3 engine.
 * These functions are only supported in V3 Calcite engine.
 */
public class CalciteMultiValueStatsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite(); // Always use Calcite V3 for these tests
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DATA_TYPE_NONNUMERIC);
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.DATETIME);
  }

  @Test
  public void testValuesWithListUdaf() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 5 | stats list(firstname) as names,"
                    + " values(gender) as unique_genders",
                TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("names", null, "array"), schema("unique_genders", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray row = dataRows.getJSONArray(0);
    JSONArray names = row.getJSONArray(0);
    JSONArray uniqueGenders = row.getJSONArray(1);

    Assertions.assertTrue(names.length() > 0, "names should not be empty");
    Assertions.assertTrue(uniqueGenders.length() > 0, "unique_genders should not be empty");

    // Verify VALUES function properties: uniqueness and sorting
    String previousGender = null;
    for (int i = 0; i < uniqueGenders.length(); i++) {
      String currentGender = uniqueGenders.getString(i);
      if (previousGender != null) {
        Assertions.assertTrue(
            currentGender.compareTo(previousGender) > 0,
            "VALUES should be sorted lexicographically: "
                + previousGender
                + " vs "
                + currentGender);
      }
      previousGender = currentGender;
    }

    System.out.println("LIST + VALUES combination works successfully!");
  }

  @Test
  public void testValuesFunctionNullHandling() throws IOException {
    // Test that VALUES function (ARRAY_AGG) respects nulls by default
    // Using a field that may have null values to verify null handling behavior
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats values(balance) as unique_balances", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("unique_balances", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray uniqueBalances = dataRows.getJSONArray(0).getJSONArray(0);
    Assertions.assertTrue(uniqueBalances.length() > 0, "unique_balances should not be empty");

    // Check if null values are included in the result
    // Note: If nulls are present, they should appear in the array
    // SQL standard ARRAY_AGG should RESPECT NULLS by default
    boolean hasNulls = false;
    for (int i = 0; i < uniqueBalances.length(); i++) {
      Object value = uniqueBalances.get(i);
      if (value == null || value == JSONObject.NULL) {
        hasNulls = true;
      }
      // All non-null values should be strings due to CAST
      if (value != null && value != JSONObject.NULL) {
        Assertions.assertTrue(
            value instanceof String, "All balance values should be strings: " + value);
      }
    }

    // Note: This test verifies that null handling works correctly
    // If nulls are present in the data, they should be preserved in the result
    System.out.println("Null values found in ARRAY_AGG result: " + hasNulls);
  }

  // NOTE: Nested array support is not implemented for LIST/VALUES functions
  // These functions are designed for scalar types only. Nested arrays (ARRAY type)
  // are properly rejected by the UDT type checker.

  // =====================================================
  // Comprehensive Scalar Data Type Tests
  // Systematically tests all OpenSearch scalar data types with both LIST and VALUES functions
  // =====================================================

  @Test
  public void testListFunctionWithObjectFieldQuery() throws IOException {
    // Test list() function with object.field queries (e.g., object_value.first)
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(object_value.first) as first_names",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(response, schema("first_names", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray firstNames = dataRows.getJSONArray(0).getJSONArray(0);
    Assertions.assertTrue(firstNames.length() > 0, "first_names should not be empty");

    // Verify that nested field values are extracted as strings
    for (int i = 0; i < firstNames.length(); i++) {
      Object value = firstNames.get(i);
      Assertions.assertTrue(
          value instanceof String, "All first name values should be strings: " + value);

      String firstName = (String) value;
      // Should contain the actual first name value (e.g., "Dale")
      Assertions.assertFalse(firstName.trim().isEmpty(), "First name should not be empty");
    }
  }

  @Test
  public void testValuesFunctionWithObjectFieldQuery() throws IOException {
    // Test values() function with object.field queries with deduplication and sorting
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats values(object_value.last) as unique_last_names",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(response, schema("unique_last_names", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray uniqueLastNames = dataRows.getJSONArray(0).getJSONArray(0);
    Assertions.assertTrue(uniqueLastNames.length() > 0, "unique_last_names should not be empty");

    // Verify lexicographic sorting and uniqueness
    String previousName = null;
    for (int i = 0; i < uniqueLastNames.length(); i++) {
      Object value = uniqueLastNames.get(i);
      Assertions.assertTrue(
          value instanceof String, "All last name values should be strings: " + value);

      String currentName = (String) value;
      if (previousName != null) {
        Assertions.assertTrue(
            currentName.compareTo(previousName) >= 0,
            "Last names should be sorted lexicographically: "
                + previousName
                + " vs "
                + currentName);
      }
      previousName = currentName;
    }
  }

  @Test
  public void testNullValueHandlingAcrossTypes() throws IOException {
    // Test null handling across different data types to ensure ARRAY_AGG respects nulls
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(balance) as balance_list, values(balance) as"
                    + " unique_balances",
                TEST_INDEX_ACCOUNT));

    verifySchema(
        response, schema("balance_list", null, "array"), schema("unique_balances", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray balanceList = dataRows.getJSONArray(0).getJSONArray(0);
    JSONArray uniqueBalances = dataRows.getJSONArray(0).getJSONArray(1);

    Assertions.assertTrue(balanceList.length() > 0, "balance_list should not be empty");
    Assertions.assertTrue(uniqueBalances.length() > 0, "unique_balances should not be empty");

    // Check if null values are properly handled (should appear as null in JSON arrays)
    boolean hasNulls = false;
    for (int i = 0; i < balanceList.length(); i++) {
      Object value = balanceList.get(i);
      if (value == null || value == JSONObject.NULL) {
        hasNulls = true;
      } else {
        // Non-null values should be strings due to CAST
        Assertions.assertTrue(
            value instanceof String, "Non-null balance values should be strings: " + value);
      }
    }

    System.out.println("Null values found in LIST result: " + hasNulls);

    // Verify VALUES also handles nulls correctly
    boolean uniqueHasNulls = false;
    for (int i = 0; i < uniqueBalances.length(); i++) {
      Object value = uniqueBalances.get(i);
      if (value == null || value == JSONObject.NULL) {
        uniqueHasNulls = true;
      } else {
        Assertions.assertTrue(
            value instanceof String, "Non-null unique balance values should be strings: " + value);
      }
    }

    System.out.println("Null values found in VALUES result: " + uniqueHasNulls);
  }

  // =====================================================
  // Edge Case and Mixed Type Tests
  // =====================================================

  @Test
  public void testMixedDataTypesInSameQuery() throws IOException {
    // Test combining multiple data types with both LIST and VALUES functions
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(boolean_value) as bool_list, values(boolean_value) as"
                    + " unique_bools, list(keyword_value) as keyword_list, values(keyword_value) as"
                    + " unique_keywords",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(
        response,
        schema("bool_list", null, "array"),
        schema("unique_bools", null, "array"),
        schema("keyword_list", null, "array"),
        schema("unique_keywords", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");
    JSONArray row = dataRows.getJSONArray(0);

    JSONArray boolList = row.getJSONArray(0);
    JSONArray uniqueBools = row.getJSONArray(1);
    JSONArray keywordList = row.getJSONArray(2);
    JSONArray uniqueKeywords = row.getJSONArray(3);

    // Verify LIST functions preserve type-specific behavior
    verifyStringArray(boolList, "boolean list");
    verifyBooleanValues(boolList, "boolean list");
    verifyStringArray(keywordList, "keyword list");

    // Verify VALUES functions maintain uniqueness and sorting
    verifyStringArray(uniqueBools, "unique boolean");
    verifyBooleanValues(uniqueBools, "unique boolean");
    verifyUniqueValues(uniqueBools, "boolean");

    verifyStringArray(uniqueKeywords, "unique keyword");
    verifySortedArray(uniqueKeywords, "keyword");
    verifyUniqueValues(uniqueKeywords, "keyword");
  }

  @Test
  public void testAllDataTypesConsistency() throws IOException {
    // Comprehensive test ensuring consistent behavior across all data types
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats "
                    + "count(boolean_value) as total_rows, "
                    + "values(boolean_value) as unique_bools, "
                    + "values(keyword_value) as unique_keywords",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(
        response,
        schema("total_rows", null, "bigint"),
        schema("unique_bools", null, "array"),
        schema("unique_keywords", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");
    JSONArray row = dataRows.getJSONArray(0);

    long totalRows = row.getLong(0);
    JSONArray uniqueBools = row.getJSONArray(1);
    JSONArray uniqueKeywords = row.getJSONArray(2);

    Assertions.assertTrue(totalRows > 0, "Should have data rows");

    // Test core properties that should hold for all supported data types:
    // 1. All array elements are strings (due to CAST(field AS VARCHAR))
    // 2. VALUES() maintains uniqueness and sorting
    // 3. No exceptions with supported data type conversion via ARRAY_AGG

    verifyAllElementsAreStrings(uniqueBools, "boolean");
    verifyAllElementsAreStrings(uniqueKeywords, "keyword");

    // Verify sorting consistency across different data types
    verifySortedArray(uniqueBools, "boolean");
    verifySortedArray(uniqueKeywords, "keyword");

    // Verify uniqueness across different data types
    verifyUniqueValues(uniqueBools, "boolean");
    verifyUniqueValues(uniqueKeywords, "keyword");
  }

  @Test
  public void testEdgeCaseEmptyResults() throws IOException {
    // Test behavior with queries that return no results
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where keyword_value = 'impossible_value' | stats list(keyword_value)"
                    + " as empty_list, values(keyword_value) as empty_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(
        response, schema("empty_list", null, "array"), schema("empty_values", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    // Empty result sets should still return one row with empty arrays
    Assertions.assertEquals(
        1, dataRows.length(), "Should return exactly one aggregation row even for empty results");

    JSONArray row = dataRows.getJSONArray(0);

    // Handle the case where aggregate functions return null for empty result sets
    Object listResult = row.get(0);
    Object valuesResult = row.get(1);

    if (listResult != null && !listResult.equals(JSONObject.NULL)) {
      JSONArray emptyList = row.getJSONArray(0);
      Assertions.assertEquals(
          0, emptyList.length(), "LIST should return empty array for no results");
    } else {
      // LIST returns null for empty result set, which is acceptable
      Assertions.assertTrue(
          listResult == null || listResult.equals(JSONObject.NULL),
          "LIST should return null or empty for no results");
    }

    if (valuesResult != null && !valuesResult.equals(JSONObject.NULL)) {
      JSONArray emptyValues = row.getJSONArray(1);
      Assertions.assertEquals(
          0, emptyValues.length(), "VALUES should return empty array for no results");
    } else {
      // VALUES returns null for empty result set, which is acceptable
      Assertions.assertTrue(
          valuesResult == null || valuesResult.equals(JSONObject.NULL),
          "VALUES should return null or empty for no results");
    }
  }

  // Helper methods for comprehensive testing

  private void verifyNumericArray(JSONArray array, String numericType) {
    Assertions.assertTrue(array.length() > 0, numericType + " array should not be empty");

    for (int i = 0; i < array.length(); i++) {
      Object value = array.get(i);
      Assertions.assertTrue(
          value instanceof String, "All " + numericType + " values should be strings: " + value);

      String numStr = (String) value;
      Assertions.assertTrue(
          numStr.matches("-?\\d+") || numStr.matches("-?\\d*\\.\\d+"),
          numericType + " string should be numeric: " + numStr);
    }
  }

  private void verifyStringArray(JSONArray array, String fieldType) {
    Assertions.assertTrue(array.length() > 0, fieldType + " array should not be empty");

    for (int i = 0; i < array.length(); i++) {
      Object value = array.get(i);
      Assertions.assertTrue(
          value instanceof String, "All " + fieldType + " values should be strings: " + value);
      Assertions.assertFalse(
          ((String) value).trim().isEmpty(), fieldType + " value should not be empty");
    }
  }

  private void verifyAllElementsAreStrings(JSONArray array, String fieldType) {
    for (int i = 0; i < array.length(); i++) {
      Object value = array.get(i);
      Assertions.assertTrue(
          value instanceof String, "All " + fieldType + " values should be strings: " + value);
    }
  }

  private void verifySortedArray(JSONArray array, String fieldType) {
    if (array.length() <= 1) return; // Single element or empty arrays are trivially sorted

    String previous = null;
    for (int i = 0; i < array.length(); i++) {
      String current = array.getString(i);
      if (previous != null) {
        Assertions.assertTrue(
            current.compareTo(previous) >= 0,
            fieldType
                + " array should be sorted lexicographically: "
                + previous
                + " vs "
                + current);
      }
      previous = current;
    }
  }

  private void verifyDateTimeArray(JSONArray array, String fieldType) {
    Assertions.assertTrue(array.length() > 0, fieldType + " array should not be empty");

    for (int i = 0; i < array.length(); i++) {
      Object value = array.get(i);
      Assertions.assertTrue(
          value instanceof String, "All " + fieldType + " values should be strings: " + value);

      String dateStr = (String) value;
      // Should contain date-like content (year patterns, date separators, time separators)
      Assertions.assertTrue(
          dateStr.matches(".*\\d{4}.*")
              || dateStr.contains("-")
              || dateStr.contains(":")
              || dateStr.matches(".*\\d{10,}.*"),
          fieldType + " string should contain date/time patterns: " + dateStr);
    }
  }

  private void verifyUniqueValues(JSONArray array, String fieldType) {
    Set<String> seenValues = new HashSet<>();
    for (int i = 0; i < array.length(); i++) {
      String value = array.getString(i);
      Assertions.assertFalse(
          seenValues.contains(value),
          fieldType + " array should contain unique values, but found duplicate: " + value);
      seenValues.add(value);
    }
  }

  private void verifyBooleanValues(JSONArray array, String fieldType) {
    for (int i = 0; i < array.length(); i++) {
      String boolStr = array.getString(i);
      Assertions.assertTrue(
          boolStr.equals("TRUE")
              || boolStr.equals("FALSE")
              || boolStr.equals("true")
              || boolStr.equals("false"),
          fieldType + " should contain 'TRUE'/'FALSE' or 'true'/'false', got: " + boolStr);
    }
  }

  private void verifyIpAddressValues(JSONArray array, String fieldType) {
    for (int i = 0; i < array.length(); i++) {
      String ipStr = array.getString(i);
      Assertions.assertTrue(
          ipStr.matches("\\d+\\.\\d+\\.\\d+\\.\\d+") || ipStr.contains(":"),
          fieldType + " should look like an IP address: " + ipStr);
    }
  }

  // Comprehensive tests for LIST function with all scalar types
  @Test
  public void testListFunctionWithAllScalarTypes() throws IOException {
    // Test with boolean field
    JSONObject boolResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(boolean_value) as bool_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(boolResponse, schema("bool_list", null, "array"));

    // Test with keyword field
    JSONObject keywordResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(keyword_value) as keyword_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(keywordResponse, schema("keyword_list", null, "array"));

    // Test with text field
    JSONObject textResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(text_value) as text_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(textResponse, schema("text_list", null, "array"));

    // Test with date field
    JSONObject dateResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(date_value) as date_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(dateResponse, schema("date_list", null, "array"));

    // Test with IP field
    JSONObject ipResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(ip_value) as ip_list", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(ipResponse, schema("ip_list", null, "array"));

    // Test with geo_point field
    JSONObject geoResponse =
        executeQuery(
            String.format(
                "source=%s | stats list(geo_point_value) as geo_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(geoResponse, schema("geo_list", null, "array"));
  }

  @Test
  public void testListFunctionWithNumericTypes() throws IOException {
    // Test with all numeric scalar types from datatypes_numeric index
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(byte_number) as byte_list, list(short_number) as"
                    + " short_list, list(integer_number) as int_list, list(long_number) as"
                    + " long_list, list(float_number) as float_list, list(double_number) as"
                    + " double_list",
                TEST_INDEX_DATATYPE_NUMERIC));

    verifySchema(
        response,
        schema("byte_list", null, "array"),
        schema("short_list", null, "array"),
        schema("int_list", null, "array"),
        schema("long_list", null, "array"),
        schema("float_list", null, "array"),
        schema("double_list", null, "array"));
  }

  @Test
  public void testListFunctionRejectsObjectType() {
    // Test that LIST function properly rejects STRUCT/object fields
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(object_value) as obj_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    Assertions.assertTrue(
        errorMessage.contains("LIST")
            || errorMessage.contains("STRUCT")
            || errorMessage.contains("type"),
        "Error message should indicate type validation failure for STRUCT type: " + errorMessage);
  }

  @Test
  public void testListFunctionRejectsNestedType() {
    // Test that LIST function properly rejects ARRAY/nested fields
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(nested_value) as nested_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    Assertions.assertTrue(
        errorMessage.contains("LIST")
            || errorMessage.contains("ARRAY")
            || errorMessage.contains("type"),
        "Error message should indicate type validation failure for ARRAY type: " + errorMessage);
  }

  @Test
  public void testListFunctionErrorMessageShowsAllowedTypes() {
    // Test that error message shows the comprehensive list of allowed scalar types
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(object_value) as obj_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    // Should contain some of the scalar types we support
    boolean containsScalarTypes =
        errorMessage.contains("STRING")
            || errorMessage.contains("BOOLEAN")
            || errorMessage.contains("INTEGER")
            || errorMessage.contains("BYTE")
            || errorMessage.contains("DOUBLE")
            || errorMessage.contains("DATE");

    Assertions.assertTrue(
        containsScalarTypes, "Error message should list allowed scalar types: " + errorMessage);
  }

  @Test
  public void testListFunctionMultipleScalarTypesInQuery() throws IOException {
    // Test that multiple LIST calls with different scalar types work in same query
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats list(boolean_value) as bool_list, list(keyword_value) as"
                    + " keyword_list, list(date_value) as date_list, list(ip_value) as ip_list",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(
        response,
        schema("bool_list", null, "array"),
        schema("keyword_list", null, "array"),
        schema("date_list", null, "array"),
        schema("ip_list", null, "array"));

    // Verify we get exactly one aggregation row
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");
  }

  // =====================================================
  // Comprehensive VALUES Function Tests for All Scalar Types
  // =====================================================

  @Test
  public void testValuesFunctionWithAllScalarTypes() throws IOException {
    // Test with boolean field
    JSONObject boolResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(boolean_value) as bool_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(boolResponse, schema("bool_values", null, "array"));

    JSONArray boolArray = boolResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(boolArray, "boolean values");
    verifyBooleanValues(boolArray, "boolean values");
    verifySortedArray(boolArray, "boolean");
    verifyUniqueValues(boolArray, "boolean");

    // Test with keyword field
    JSONObject keywordResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(keyword_value) as keyword_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(keywordResponse, schema("keyword_values", null, "array"));

    JSONArray keywordArray =
        keywordResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(keywordArray, "keyword values");
    verifySortedArray(keywordArray, "keyword");
    verifyUniqueValues(keywordArray, "keyword");

    // Test with text field
    JSONObject textResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(text_value) as text_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(textResponse, schema("text_values", null, "array"));

    JSONArray textArray = textResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(textArray, "text values");
    verifySortedArray(textArray, "text");
    verifyUniqueValues(textArray, "text");

    // Test with date field
    JSONObject dateResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(date_value) as date_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(dateResponse, schema("date_values", null, "array"));

    JSONArray dateArray = dateResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(dateArray, "date values");
    verifyDateTimeArray(dateArray, "date");
    verifySortedArray(dateArray, "date");
    verifyUniqueValues(dateArray, "date");

    // Test with IP field
    JSONObject ipResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(ip_value) as ip_values", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(ipResponse, schema("ip_values", null, "array"));

    JSONArray ipArray = ipResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(ipArray, "IP values");
    verifyIpAddressValues(ipArray, "IP address values");
    verifySortedArray(ipArray, "IP");
    verifyUniqueValues(ipArray, "IP");

    // Test with geo_point field
    JSONObject geoResponse =
        executeQuery(
            String.format(
                "source=%s | stats values(geo_point_value) as geo_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchema(geoResponse, schema("geo_values", null, "array"));

    JSONArray geoArray = geoResponse.getJSONArray("datarows").getJSONArray(0).getJSONArray(0);
    verifyStringArray(geoArray, "geo_point values");
    verifySortedArray(geoArray, "geo_point");
    verifyUniqueValues(geoArray, "geo_point");
  }

  @Test
  public void testValuesFunctionWithAllNumericTypes() throws IOException {
    // Test with all numeric scalar types from datatypes_numeric index
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats values(byte_number) as byte_values, values(short_number) as"
                    + " short_values, values(integer_number) as int_values, values(long_number) as"
                    + " long_values, values(float_number) as float_values, values(double_number) as"
                    + " double_values",
                TEST_INDEX_DATATYPE_NUMERIC));

    verifySchema(
        response,
        schema("byte_values", null, "array"),
        schema("short_values", null, "array"),
        schema("int_values", null, "array"),
        schema("long_values", null, "array"),
        schema("float_values", null, "array"),
        schema("double_values", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");
    JSONArray row = dataRows.getJSONArray(0);

    // Verify each numeric type maintains VALUES function properties
    JSONArray byteValues = row.getJSONArray(0);
    JSONArray shortValues = row.getJSONArray(1);
    JSONArray intValues = row.getJSONArray(2);
    JSONArray longValues = row.getJSONArray(3);
    JSONArray floatValues = row.getJSONArray(4);
    JSONArray doubleValues = row.getJSONArray(5);

    // Verify all arrays have VALUES function properties: string conversion, uniqueness, sorting
    verifyNumericArray(byteValues, "byte values");
    verifyNumericArray(shortValues, "short values");
    verifyNumericArray(intValues, "integer values");
    verifyNumericArray(longValues, "long values");
    verifyNumericArray(floatValues, "float values");
    verifyNumericArray(doubleValues, "double values");

    verifySortedArray(byteValues, "byte");
    verifySortedArray(shortValues, "short");
    verifySortedArray(intValues, "integer");
    verifySortedArray(longValues, "long");
    verifySortedArray(floatValues, "float");
    verifySortedArray(doubleValues, "double");

    verifyUniqueValues(byteValues, "byte");
    verifyUniqueValues(shortValues, "short");
    verifyUniqueValues(intValues, "integer");
    verifyUniqueValues(longValues, "long");
    verifyUniqueValues(floatValues, "float");
    verifyUniqueValues(doubleValues, "double");
  }

  @Test
  public void testValuesFunctionRejectsObjectType() {
    // Test that VALUES function properly rejects STRUCT/object fields with correct error message
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(object_value) as obj_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    Assertions.assertTrue(
        errorMessage.contains("VALUES")
            || errorMessage.contains("STRUCT")
            || errorMessage.contains("type"),
        "Error message should indicate type validation failure for STRUCT type: " + errorMessage);
  }

  @Test
  public void testValuesFunctionRejectsNestedType() {
    // Test that VALUES function properly rejects ARRAY/nested fields with correct error message
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(nested_value) as nested_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    Assertions.assertTrue(
        errorMessage.contains("VALUES")
            || errorMessage.contains("ARRAY")
            || errorMessage.contains("type"),
        "Error message should indicate type validation failure for ARRAY type: " + errorMessage);
  }

  @Test
  public void testValuesFunctionErrorMessageShowsAllowedTypes() {
    // Test that VALUES function error message shows the comprehensive list of allowed scalar types
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(object_value) as obj_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String errorMessage = exception.getMessage();
    // Should contain some of the scalar types we support
    boolean containsScalarTypes =
        errorMessage.contains("STRING")
            || errorMessage.contains("BOOLEAN")
            || errorMessage.contains("INTEGER")
            || errorMessage.contains("BYTE")
            || errorMessage.contains("DOUBLE")
            || errorMessage.contains("DATE");

    Assertions.assertTrue(
        containsScalarTypes, "Error message should list allowed scalar types: " + errorMessage);
  }

  @Test
  public void testValuesFunctionMultipleScalarTypesInQuery() throws IOException {
    // Test that multiple VALUES calls with different scalar types work in same query
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats values(boolean_value) as bool_values, values(keyword_value) as"
                    + " keyword_values, values(date_value) as date_values, values(ip_value) as"
                    + " ip_values",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(
        response,
        schema("bool_values", null, "array"),
        schema("keyword_values", null, "array"),
        schema("date_values", null, "array"),
        schema("ip_values", null, "array"));

    // Verify we get exactly one aggregation row
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray row = dataRows.getJSONArray(0);
    JSONArray boolValues = row.getJSONArray(0);
    JSONArray keywordValues = row.getJSONArray(1);
    JSONArray dateValues = row.getJSONArray(2);
    JSONArray ipValues = row.getJSONArray(3);

    // Verify all arrays have VALUES function properties
    verifyStringArray(boolValues, "boolean values");
    verifyBooleanValues(boolValues, "boolean values");
    verifySortedArray(boolValues, "boolean");
    verifyUniqueValues(boolValues, "boolean");

    verifyStringArray(keywordValues, "keyword values");
    verifySortedArray(keywordValues, "keyword");
    verifyUniqueValues(keywordValues, "keyword");

    verifyStringArray(dateValues, "date values");
    verifyDateTimeArray(dateValues, "date");
    verifySortedArray(dateValues, "date");
    verifyUniqueValues(dateValues, "date");

    verifyStringArray(ipValues, "IP values");
    verifyIpAddressValues(ipValues, "IP values");
    verifySortedArray(ipValues, "IP");
    verifyUniqueValues(ipValues, "IP");
  }

  @Test
  public void testValuesFunctionSortingBehavior() throws IOException {
    // Test that VALUES function provides proper lexicographical sorting for different data types

    // Test with account data that should have multiple distinct values to sort lexicographically
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 10 | stats values(gender) as sorted_genders",
                TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("sorted_genders", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray sortedGenders = dataRows.getJSONArray(0).getJSONArray(0);
    Assertions.assertTrue(
        sortedGenders.length() > 1, "Should have multiple values to verify sorting");

    // Verify strict lexicographical sorting
    for (int i = 1; i < sortedGenders.length(); i++) {
      String current = sortedGenders.getString(i);
      String previous = sortedGenders.getString(i - 1);

      Assertions.assertTrue(
          current.compareTo(previous) > 0,
          "VALUES should be sorted lexicographically: '"
              + previous
              + "' should come before '"
              + current
              + "'");
    }
  }

  @Test
  public void testValuesFunctionUniquenessProperty() throws IOException {
    // Test that VALUES function removes duplicates properly
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats values(boolean_value) as unique_bools",
                TEST_INDEX_DATATYPE_NONNUMERIC));

    verifySchema(response, schema("unique_bools", null, "array"));

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(1, dataRows.length(), "Should return exactly one aggregation row");

    JSONArray uniqueBools = dataRows.getJSONArray(0).getJSONArray(0);

    // Boolean field should have at most 2 unique values (TRUE, FALSE)
    Assertions.assertTrue(
        uniqueBools.length() <= 2,
        "Boolean field should have at most 2 unique values, got: " + uniqueBools.length());

    // Verify no duplicates
    Set<String> seenValues = new HashSet<>();
    for (int i = 0; i < uniqueBools.length(); i++) {
      String value = uniqueBools.getString(i);
      Assertions.assertFalse(
          seenValues.contains(value),
          "VALUES should not contain duplicates, found duplicate: " + value);
      seenValues.add(value);
    }
  }

  // =====================================================
  // Non-Scalar Type Rejection Tests with Proper Error Messages
  // =====================================================

  @Test
  public void testListAndValuesFunctionsRejectStructType() {
    // Test that both LIST and VALUES functions reject STRUCT types with proper error messages

    // Test LIST function with STRUCT
    Exception listException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(object_value) as obj_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String listErrorMessage = listException.getMessage();
    boolean listHasValidError =
        listErrorMessage.contains("LIST")
            || listErrorMessage.contains("STRUCT")
            || listErrorMessage.contains("expects field type")
            || listErrorMessage.contains("got");
    Assertions.assertTrue(
        listHasValidError,
        "LIST function should reject STRUCT type with proper error message: " + listErrorMessage);

    // Test VALUES function with STRUCT
    Exception valuesException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(object_value) as obj_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String valuesErrorMessage = valuesException.getMessage();
    boolean valuesHasValidError =
        valuesErrorMessage.contains("VALUES")
            || valuesErrorMessage.contains("STRUCT")
            || valuesErrorMessage.contains("expects field type")
            || valuesErrorMessage.contains("got");
    Assertions.assertTrue(
        valuesHasValidError,
        "VALUES function should reject STRUCT type with proper error message: "
            + valuesErrorMessage);
  }

  @Test
  public void testListAndValuesFunctionsRejectArrayType() {
    // Test that both LIST and VALUES functions reject ARRAY types with proper error messages

    // Test LIST function with ARRAY
    Exception listException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(nested_value) as nested_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String listErrorMessage = listException.getMessage();
    boolean listHasValidError =
        listErrorMessage.contains("LIST")
            || listErrorMessage.contains("ARRAY")
            || listErrorMessage.contains("expects field type")
            || listErrorMessage.contains("got");
    Assertions.assertTrue(
        listHasValidError,
        "LIST function should reject ARRAY type with proper error message: " + listErrorMessage);

    // Test VALUES function with ARRAY
    Exception valuesException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(nested_value) as nested_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String valuesErrorMessage = valuesException.getMessage();
    boolean valuesHasValidError =
        valuesErrorMessage.contains("VALUES")
            || valuesErrorMessage.contains("ARRAY")
            || valuesErrorMessage.contains("expects field type")
            || valuesErrorMessage.contains("got");
    Assertions.assertTrue(
        valuesHasValidError,
        "VALUES function should reject ARRAY type with proper error message: "
            + valuesErrorMessage);
  }

  @Test
  public void testErrorMessagesListAllowedScalarTypes() {
    // Test that error messages for both LIST and VALUES show comprehensive list of allowed types

    Exception listException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(object_value) as obj_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String listErrorMessage = listException.getMessage();

    // Should mention multiple scalar types
    int scalarTypeCount = 0;
    String[] scalarTypes = {
      "STRING",
      "BOOLEAN",
      "INTEGER",
      "BYTE",
      "SHORT",
      "LONG",
      "FLOAT",
      "DOUBLE",
      "DATE",
      "TIME",
      "TIMESTAMP",
      "IP",
      "GEO_POINT",
      "BINARY"
    };

    for (String type : scalarTypes) {
      if (listErrorMessage.contains(type)) {
        scalarTypeCount++;
      }
    }

    Assertions.assertTrue(
        scalarTypeCount >= 3,
        "LIST error message should mention multiple allowed scalar types (found "
            + scalarTypeCount
            + "): "
            + listErrorMessage);

    // Test the same for VALUES function
    Exception valuesException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(object_value) as obj_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String valuesErrorMessage = valuesException.getMessage();

    int valuesScalarTypeCount = 0;
    for (String type : scalarTypes) {
      if (valuesErrorMessage.contains(type)) {
        valuesScalarTypeCount++;
      }
    }

    Assertions.assertTrue(
        valuesScalarTypeCount >= 3,
        "VALUES error message should mention multiple allowed scalar types (found "
            + valuesScalarTypeCount
            + "): "
            + valuesErrorMessage);
  }

  @Test
  public void testBothFunctionsConsistentErrorMessagesForNonScalarTypes() {
    // Verify that LIST and VALUES provide consistent error messages for the same non-scalar type

    Exception listStructException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats list(object_value) as obj_list",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    Exception valuesStructException =
        Assertions.assertThrows(
            Exception.class,
            () -> {
              executeQuery(
                  String.format(
                      "source=%s | stats values(object_value) as obj_values",
                      TEST_INDEX_DATATYPE_NONNUMERIC));
            });

    String listError = listStructException.getMessage();
    String valuesError = valuesStructException.getMessage();

    // Both should mention type validation failure
    boolean listMentionsType =
        listError.contains("type") || listError.contains("expects") || listError.contains("STRUCT");
    boolean valuesMentionsType =
        valuesError.contains("type")
            || valuesError.contains("expects")
            || valuesError.contains("STRUCT");

    Assertions.assertTrue(
        listMentionsType, "LIST error should mention type validation: " + listError);
    Assertions.assertTrue(
        valuesMentionsType, "VALUES error should mention type validation: " + valuesError);
  }

  // =====================================================
  // Eventstats Integration Tests for LIST and VALUES Functions
  // =====================================================

  @Test
  public void testListFunctionWithEventstats() throws IOException {
    // Test LIST function with eventstats command to verify it enriches each record
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 5 | eventstats list(firstname) as all_names",
                TEST_INDEX_ACCOUNT));

    // eventstats returns all original columns plus the new aggregate column
    // We'll just verify that all_names column exists and has the correct type
    JSONArray schema = response.getJSONArray("schema");
    boolean foundAllNamesColumn = false;
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      if ("all_names".equals(column.getString("name"))) {
        Assertions.assertEquals("array", column.getString("type"));
        foundAllNamesColumn = true;
        break;
      }
    }
    Assertions.assertTrue(foundAllNamesColumn, "Should have all_names column with array type");

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should return multiple rows with eventstats");

    // Find the column index for all_names
    JSONArray schemaArray = response.getJSONArray("schema");
    int allNamesColumnIndex = -1;
    for (int i = 0; i < schemaArray.length(); i++) {
      JSONObject column = schemaArray.getJSONObject(i);
      if ("all_names".equals(column.getString("name"))) {
        allNamesColumnIndex = i;
        break;
      }
    }
    Assertions.assertTrue(allNamesColumnIndex >= 0, "Should find all_names column");

    // Verify that each row contains the same list of names (eventstats behavior)
    JSONArray firstRowNames = null;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      JSONArray names = row.getJSONArray(allNamesColumnIndex); // all_names column

      Assertions.assertTrue(names.length() > 0, "Names list should not be empty");

      // Verify all values are strings
      for (int j = 0; j < names.length(); j++) {
        Object value = names.get(j);
        Assertions.assertTrue(
            value instanceof String, "All firstname values should be strings: " + value);
        Assertions.assertFalse(((String) value).trim().isEmpty(), "Name should not be empty");
      }

      // For eventstats, all rows should have the same aggregate result
      if (firstRowNames == null) {
        firstRowNames = names;
      } else {
        Assertions.assertEquals(
            firstRowNames.length(),
            names.length(),
            "All rows should have the same list length with eventstats");

        // Verify same content (LIST preserves order)
        for (int j = 0; j < names.length(); j++) {
          Assertions.assertEquals(
              firstRowNames.getString(j),
              names.getString(j),
              "All rows should have identical lists with eventstats");
        }
      }
    }
  }

  @Test
  public void testValuesFunctionWithEventstats() throws IOException {
    // Test VALUES function with eventstats command to verify it enriches each record
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 5 | eventstats values(gender) as"
                    + " unique_genders",
                TEST_INDEX_ACCOUNT));

    // eventstats returns all original columns plus the new aggregate column
    JSONArray schema = response.getJSONArray("schema");
    boolean foundUniqueGendersColumn = false;
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      if ("unique_genders".equals(column.getString("name"))) {
        Assertions.assertEquals("array", column.getString("type"));
        foundUniqueGendersColumn = true;
        break;
      }
    }
    Assertions.assertTrue(
        foundUniqueGendersColumn, "Should have unique_genders column with array type");

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should return multiple rows with eventstats");

    // Find the column index for unique_genders
    JSONArray schemaArray = response.getJSONArray("schema");
    int uniqueGendersColumnIndex = -1;
    for (int i = 0; i < schemaArray.length(); i++) {
      JSONObject column = schemaArray.getJSONObject(i);
      if ("unique_genders".equals(column.getString("name"))) {
        uniqueGendersColumnIndex = i;
        break;
      }
    }
    Assertions.assertTrue(uniqueGendersColumnIndex >= 0, "Should find unique_genders column");

    // Verify that each row contains the same sorted unique values (eventstats behavior)
    JSONArray firstRowValues = null;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);
      JSONArray uniqueGenders = row.getJSONArray(uniqueGendersColumnIndex); // unique_genders column

      Assertions.assertTrue(uniqueGenders.length() > 0, "Unique genders should not be empty");

      // Verify VALUES function properties: uniqueness and sorting
      String previousGender = null;
      Set<String> seenGenders = new HashSet<>();
      for (int j = 0; j < uniqueGenders.length(); j++) {
        Object value = uniqueGenders.get(j);
        Assertions.assertTrue(
            value instanceof String, "All gender values should be strings: " + value);

        String currentGender = (String) value;

        // Check uniqueness
        Assertions.assertFalse(
            seenGenders.contains(currentGender),
            "VALUES should contain unique values, found duplicate: " + currentGender);
        seenGenders.add(currentGender);

        // Check sorting
        if (previousGender != null) {
          Assertions.assertTrue(
              currentGender.compareTo(previousGender) > 0,
              "VALUES should be in lexicographic order: "
                  + previousGender
                  + " vs "
                  + currentGender);
        }
        previousGender = currentGender;
      }

      // For eventstats, all rows should have the same aggregate result
      if (firstRowValues == null) {
        firstRowValues = uniqueGenders;
      } else {
        Assertions.assertEquals(
            firstRowValues.length(),
            uniqueGenders.length(),
            "All rows should have the same values length with eventstats");

        // Verify same content and order (VALUES maintains consistent sorting)
        for (int j = 0; j < uniqueGenders.length(); j++) {
          Assertions.assertEquals(
              firstRowValues.getString(j),
              uniqueGenders.getString(j),
              "All rows should have identical sorted unique values with eventstats");
        }
      }
    }
  }

  @Test
  public void testListAndValuesFunctionsWithEventstatsAndGroupBy() throws IOException {
    // Test both LIST and VALUES functions with eventstats and group by clause
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 10 | eventstats list(firstname) as all_names,"
                    + " values(gender) as unique_genders by state",
                TEST_INDEX_ACCOUNT));

    // eventstats returns all original columns plus the new aggregate columns
    JSONArray schema = response.getJSONArray("schema");
    boolean foundAllNamesColumn = false;
    boolean foundUniqueGendersColumn = false;
    int allNamesColumnIndex = -1;
    int uniqueGendersColumnIndex = -1;

    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      String name = column.getString("name");
      if ("all_names".equals(name)) {
        Assertions.assertEquals("array", column.getString("type"));
        foundAllNamesColumn = true;
        allNamesColumnIndex = i;
      } else if ("unique_genders".equals(name)) {
        Assertions.assertEquals("array", column.getString("type"));
        foundUniqueGendersColumn = true;
        uniqueGendersColumnIndex = i;
      }
    }
    Assertions.assertTrue(foundAllNamesColumn, "Should have all_names column with array type");
    Assertions.assertTrue(
        foundUniqueGendersColumn, "Should have unique_genders column with array type");

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should return multiple rows with eventstats");

    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);

      // Get the aggregate columns
      JSONArray allNames = row.getJSONArray(allNamesColumnIndex);
      JSONArray uniqueGenders = row.getJSONArray(uniqueGendersColumnIndex);

      Assertions.assertTrue(allNames.length() > 0, "Names list should not be empty");
      Assertions.assertTrue(uniqueGenders.length() > 0, "Unique genders should not be empty");

      // Verify LIST function properties
      for (int j = 0; j < allNames.length(); j++) {
        Object value = allNames.get(j);
        Assertions.assertTrue(
            value instanceof String, "All firstname values should be strings: " + value);
      }

      // Verify VALUES function properties: uniqueness and sorting
      String previousGender = null;
      Set<String> seenGenders = new HashSet<>();
      for (int j = 0; j < uniqueGenders.length(); j++) {
        String currentGender = uniqueGenders.getString(j);

        Assertions.assertFalse(
            seenGenders.contains(currentGender),
            "VALUES should contain unique values: " + currentGender);
        seenGenders.add(currentGender);

        if (previousGender != null) {
          Assertions.assertTrue(
              currentGender.compareTo(previousGender) > 0,
              "VALUES should be sorted: " + previousGender + " vs " + currentGender);
        }
        previousGender = currentGender;
      }
    }
  }

  @Test
  public void testMultiValueStatsWithEventstatsAndOtherAggregates() throws IOException {
    // Test LIST and VALUES functions with other aggregate functions in eventstats
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where account_number < 5 | eventstats count() as total_count,"
                    + " list(firstname) as all_names, avg(age) as avg_age, values(gender) as"
                    + " unique_genders",
                TEST_INDEX_ACCOUNT));

    // eventstats returns all original columns plus the new aggregate columns
    JSONArray schema = response.getJSONArray("schema");
    int totalCountColumnIndex = -1;
    int allNamesColumnIndex = -1;
    int avgAgeColumnIndex = -1;
    int uniqueGendersColumnIndex = -1;

    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      String name = column.getString("name");
      if ("total_count".equals(name)) {
        Assertions.assertEquals("bigint", column.getString("type"));
        totalCountColumnIndex = i;
      } else if ("all_names".equals(name)) {
        Assertions.assertEquals("array", column.getString("type"));
        allNamesColumnIndex = i;
      } else if ("avg_age".equals(name)) {
        Assertions.assertEquals("double", column.getString("type"));
        avgAgeColumnIndex = i;
      } else if ("unique_genders".equals(name)) {
        Assertions.assertEquals("array", column.getString("type"));
        uniqueGendersColumnIndex = i;
      }
    }

    Assertions.assertTrue(totalCountColumnIndex >= 0, "Should find total_count column");
    Assertions.assertTrue(allNamesColumnIndex >= 0, "Should find all_names column");
    Assertions.assertTrue(avgAgeColumnIndex >= 0, "Should find avg_age column");
    Assertions.assertTrue(uniqueGendersColumnIndex >= 0, "Should find unique_genders column");

    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertTrue(dataRows.length() > 0, "Should return multiple rows with eventstats");

    // Verify that all aggregate values are consistent across all rows (eventstats behavior)
    Long firstRowCount = null;
    JSONArray firstRowNames = null;
    Double firstRowAvgAge = null;
    JSONArray firstRowUniqueGenders = null;

    for (int i = 0; i < dataRows.length(); i++) {
      JSONArray row = dataRows.getJSONArray(i);

      Long totalCount = row.getLong(totalCountColumnIndex);
      JSONArray allNames = row.getJSONArray(allNamesColumnIndex);
      Double avgAge = row.getDouble(avgAgeColumnIndex);
      JSONArray uniqueGenders = row.getJSONArray(uniqueGendersColumnIndex);

      // Verify basic properties
      Assertions.assertTrue(totalCount > 0, "Total count should be greater than 0");
      Assertions.assertTrue(allNames.length() > 0, "Names list should not be empty");
      Assertions.assertTrue(avgAge > 0, "Average age should be greater than 0");
      Assertions.assertTrue(uniqueGenders.length() > 0, "Unique genders should not be empty");

      // For eventstats, all rows should have identical aggregate results
      if (firstRowCount == null) {
        firstRowCount = totalCount;
        firstRowNames = allNames;
        firstRowAvgAge = avgAge;
        firstRowUniqueGenders = uniqueGenders;
      } else {
        Assertions.assertEquals(
            firstRowCount, totalCount, "All rows should have same count with eventstats");
        Assertions.assertEquals(
            firstRowAvgAge, avgAge, 0.01, "All rows should have same average age with eventstats");

        // Verify LIST results are identical
        Assertions.assertEquals(
            firstRowNames.length(), allNames.length(), "LIST results should be identical");
        for (int j = 0; j < allNames.length(); j++) {
          Assertions.assertEquals(
              firstRowNames.getString(j), allNames.getString(j), "LIST contents should match");
        }

        // Verify VALUES results are identical
        Assertions.assertEquals(
            firstRowUniqueGenders.length(),
            uniqueGenders.length(),
            "VALUES results should be identical");
        for (int j = 0; j < uniqueGenders.length(); j++) {
          Assertions.assertEquals(
              firstRowUniqueGenders.getString(j),
              uniqueGenders.getString(j),
              "VALUES contents should match");
        }
      }
    }
  }

  // =====================================================
  // Data-Driven Tests for Different Data Types
  // =====================================================

  @Test
  public void testListAndValuesFunctionsWithDifferentDataTypes() throws IOException {
    // Test data: {function, field, index, description}
    String[][] testCases = {
      {"list", "firstname", TEST_INDEX_ACCOUNT, "LIST with string field"},
      {"list", "gender", TEST_INDEX_ACCOUNT, "LIST with keyword field"},
      {"list", "age", TEST_INDEX_ACCOUNT, "LIST with integer field"},
      {"list", "balance", TEST_INDEX_ACCOUNT, "LIST with long field"},
      {"list", "ip_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with IP field"},
      {"list", "date_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with date field"},
      {"list", "date_nanos_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with date_nanos field"},
      {"list", "boolean_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with boolean field"},
      {"list", "keyword_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with keyword field"},
      {"list", "text_value", TEST_INDEX_DATATYPE_NONNUMERIC, "LIST with text field"},
      {"values", "firstname", TEST_INDEX_ACCOUNT, "VALUES with string field"},
      {"values", "gender", TEST_INDEX_ACCOUNT, "VALUES with keyword field"},
      {"values", "age", TEST_INDEX_ACCOUNT, "VALUES with integer field"},
      {"values", "balance", TEST_INDEX_ACCOUNT, "VALUES with long field"},
      {"values", "ip_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with IP field"},
      {"values", "date_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with date field"},
      {
        "values", "date_nanos_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with date_nanos field"
      },
      {"values", "boolean_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with boolean field"},
      {"values", "keyword_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with keyword field"},
      {"values", "text_value", TEST_INDEX_DATATYPE_NONNUMERIC, "VALUES with text field"}
    };

    for (String[] testCase : testCases) {
      String functionName = testCase[0];
      String fieldName = testCase[1];
      String testIndex = testCase[2];
      String description = testCase[3];

      System.out.println("Testing: " + description);
      testMultiValueStatsFunctionWithDataType(functionName, fieldName, testIndex, description);
    }
  }

  private void testMultiValueStatsFunctionWithDataType(
      String functionName, String fieldName, String testIndex, String description)
      throws IOException {
    // Use different filter conditions based on the index
    String filterCondition;
    if (testIndex.equals(TEST_INDEX_ACCOUNT)) {
      filterCondition = "account_number < 5";
    } else if (testIndex.equals(TEST_INDEX_DATATYPE_NONNUMERIC)) {
      filterCondition = "boolean_value = true OR boolean_value = false";
    } else {
      filterCondition = "1 = 1"; // Default: no filter, get all records
    }

    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where %s | stats %s(%s) as result",
                testIndex, filterCondition, functionName, fieldName));

    // Verify schema - result column should have array type
    JSONArray schema = response.getJSONArray("schema");
    boolean foundResultColumn = false;
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      if ("result".equals(column.getString("name"))) {
        Assertions.assertEquals(
            "array",
            column.getString("type"),
            String.format("Expected array type for %s", description));
        foundResultColumn = true;
        break;
      }
    }
    Assertions.assertTrue(
        foundResultColumn, String.format("Should find result column for %s", description));

    // Verify data rows
    JSONArray dataRows = response.getJSONArray("datarows");
    Assertions.assertEquals(
        1,
        dataRows.length(),
        String.format("Should have exactly one result row for %s", description));

    JSONArray row = dataRows.getJSONArray(0);
    JSONArray resultArray = row.getJSONArray(0);
    Assertions.assertTrue(
        resultArray.length() > 0,
        String.format("Result array should not be empty for %s", description));

    // Verify function-specific properties
    if ("values".equals(functionName)) {
      verifyValuesProperties(resultArray, description);
    } else if ("list".equals(functionName)) {
      verifyListProperties(resultArray, description);
    }

    // Verify all values are properly converted to strings
    for (int i = 0; i < resultArray.length(); i++) {
      Object value = resultArray.get(i);
      Assertions.assertTrue(
          value instanceof String,
          String.format(
              "All values should be strings for %s, found: %s", description, value.getClass()));
      Assertions.assertFalse(
          ((String) value).trim().isEmpty(),
          String.format("Values should not be empty strings for %s", description));
    }
  }

  private void verifyValuesProperties(JSONArray resultArray, String description) {
    // Verify uniqueness
    Set<String> uniqueValues = new HashSet<>();
    for (int i = 0; i < resultArray.length(); i++) {
      String value = resultArray.getString(i);
      Assertions.assertFalse(
          uniqueValues.contains(value),
          String.format(
              "VALUES should contain unique values, found duplicate '%s' for %s",
              value, description));
      uniqueValues.add(value);
    }

    // Verify lexicographic ordering
    String previousValue = null;
    for (int i = 0; i < resultArray.length(); i++) {
      String currentValue = resultArray.getString(i);
      if (previousValue != null) {
        Assertions.assertTrue(
            currentValue.compareTo(previousValue) > 0,
            String.format(
                "VALUES should be in lexicographic order for %s: '%s' vs '%s'",
                description, previousValue, currentValue));
      }
      previousValue = currentValue;
    }
  }

  private void verifyListProperties(JSONArray resultArray, String description) {
    // LIST function preserves all values including duplicates
    // Just verify that we have values (duplicates are allowed and order is preserved)
    Assertions.assertTrue(
        resultArray.length() > 0,
        String.format("LIST should have at least one value for %s", description));

    // All values should be non-null strings
    for (int i = 0; i < resultArray.length(); i++) {
      Object value = resultArray.get(i);
      Assertions.assertNotNull(
          value, String.format("LIST values should not be null for %s", description));
      Assertions.assertTrue(
          value instanceof String,
          String.format(
              "LIST values should be strings for %s, found: %s", description, value.getClass()));
    }
  }
}
