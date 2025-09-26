/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonExtractAllFunctionImplTest {

  @Test
  void testBasicFlatJsonExtraction() {
    String json = "{\"name\": \"John\", \"age\": 30, \"active\": true}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());
    assertEquals("John", result.get("name"));
    assertEquals("30", result.get("age"));
    assertEquals("true", result.get("active"));
  }

  @Test
  void testNestedObjectExtraction() {
    String json =
        "{\"user\": {\"name\": \"Alice\", \"profile\": {\"age\": 28, \"location\": \"Seattle\"}}}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());
    assertEquals("Alice", result.get("user.name"));
    assertEquals("28", result.get("user.profile.age"));
    assertEquals("Seattle", result.get("user.profile.location"));
  }

  @Test
  void testMixedNestedAndFlatFields() {
    String json =
        "{\"id\": 1, \"user\": {\"name\": \"Bob\", \"details\": {\"age\": 35, \"city\":"
            + " \"Portland\"}}, \"status\": \"active\"}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(5, result.size());
    assertEquals("1", result.get("id"));
    assertEquals("Bob", result.get("user.name"));
    assertEquals("35", result.get("user.details.age"));
    assertEquals("Portland", result.get("user.details.city"));
    assertEquals("active", result.get("status"));
  }

  @Test
  void testArrayExtraction() {
    String json = "{\"name\": \"Charlie\", \"hobbies\": [\"reading\", \"swimming\", \"coding\"]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(2, result.size());
    assertEquals("Charlie", result.get("name"));

    // Arrays are now converted to JSON string representations
    String hobbies = (String) result.get("hobbies{}");
    assertEquals("[\"reading\",\"swimming\",\"coding\"]", hobbies);
  }

  @Test
  void testNestedArrayWithObjects() {
    String json =
        "{\"users\": [{\"name\": \"Alice\", \"age\": 25}, {\"name\": \"Bob\", \"age\": 30}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(2, result.size());

    // Arrays are now converted to JSON string representations
    String nameValues = (String) result.get("users{}.name");
    assertEquals("[\"Alice\",\"Bob\"]", nameValues);

    String ageValues = (String) result.get("users{}.age");
    assertEquals("[\"25\",\"30\"]", ageValues);
  }

  @Test
  void testNestedArray() {
    String json = "{\"arr\": [1, [2, 3], [4, 5], 6]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(2, result.size());

    // Arrays are now converted to JSON string representations
    String primitiveValues = (String) result.get("arr{}");
    assertEquals("[\"1\",\"6\"]", primitiveValues);

    String nestedArrayValues = (String) result.get("arr{}{}");
    assertEquals("[\"2\",\"3\",\"4\",\"5\"]", nestedArrayValues);
  }

  @Test
  void testDeeplyNestedStructure() {
    String json = "{\"level1\": {\"level2\": {\"level3\": {\"level4\": {\"value\": \"deep\"}}}}}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(1, result.size());
    assertEquals("deep", result.get("level1.level2.level3.level4.value"));
  }

  @Test
  void testMixedDataTypes() {
    String json =
        "{\"string\": \"text\", \"integer\": 42, \"double\": 3.14, \"boolean\": false,"
            + " \"null_value\": null}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(5, result.size());
    assertEquals("text", result.get("string"));
    assertEquals("42", result.get("integer"));
    assertEquals("3.14", result.get("double"));
    assertEquals("false", result.get("boolean"));
    assertNull(result.get("null_value"));
  }

  @Test
  void testComplexRealWorldExample() {
    String json =
        "{"
            + "\"user\": {"
            + "  \"id\": 123,"
            + "  \"profile\": {"
            + "    \"name\": \"John Doe\","
            + "    \"email\": \"john@example.com\","
            + "    \"preferences\": {"
            + "      \"theme\": \"dark\","
            + "      \"notifications\": true"
            + "    }"
            + "  },"
            + "  \"roles\": [\"admin\", \"user\"]"
            + "},"
            + "\"metadata\": {"
            + "  \"created\": \"2023-01-01\","
            + "  \"version\": 1.2"
            + "}"
            + "}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(8, result.size());
    assertEquals("123", result.get("user.id"));
    assertEquals("John Doe", result.get("user.profile.name"));
    assertEquals("john@example.com", result.get("user.profile.email"));
    assertEquals("dark", result.get("user.profile.preferences.theme"));
    assertEquals("true", result.get("user.profile.preferences.notifications"));
    assertEquals("2023-01-01", result.get("metadata.created"));
    assertEquals("1.2", result.get("metadata.version"));

    // Arrays are now converted to JSON string representations
    String roles = (String) result.get("user.roles{}");
    assertEquals("[\"admin\",\"user\"]", roles);
  }

  @Test
  void testEmptyObject() {
    String json = "{}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertTrue(result.isEmpty());
  }

  @Test
  void testNestedEmptyObjects() {
    String json = "{\"outer\": {\"inner\": {}}, \"value\": 42}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(1, result.size());
    assertEquals("42", result.get("value"));
  }

  @Test
  void testArrayWithMixedTypes() {
    String json = "{\"mixed\": [\"string\", 42, true, null, {\"nested\": \"object\"}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(2, result.size());

    // Since array contains objects, values from objects are collected
    assertEquals("object", result.get("mixed{}.nested"));
  }

  @Test
  void testNullInput() {
    Object result = JsonExtractAllFunctionImpl.eval((String) null);
    assertNull(result);
  }

  @Test
  void testEmptyStringInput() {
    Object result = JsonExtractAllFunctionImpl.eval("");
    assertNull(result);
  }

  @Test
  void testWhitespaceOnlyInput() {
    Object result = JsonExtractAllFunctionImpl.eval("   ");
    assertNull(result);
  }

  @Test
  void testInvalidJsonInput() {
    Object result = JsonExtractAllFunctionImpl.eval("{invalid json}");
    assertNull(result);
  }

  @Test
  void testNonObjectJsonInput() {
    // Array at root level
    Object result1 = JsonExtractAllFunctionImpl.eval("[1, 2, 3]");
    assertNull(result1);

    // Primitive at root level
    Object result2 = JsonExtractAllFunctionImpl.eval("\"just a string\"");
    assertNull(result2);

    Object result3 = JsonExtractAllFunctionImpl.eval("42");
    assertNull(result3);
  }

  @Test
  void testNoArgumentsInput() {
    Object result = JsonExtractAllFunctionImpl.eval();
    assertNull(result);
  }

  @Test
  void testSpecialCharactersInFieldNames() {
    String json =
        "{\"field-with-dash\": \"value1\", \"field_with_underscore\": \"value2\","
            + " \"field.with.dots\": \"value3\"}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());
    assertEquals("value1", result.get("field-with-dash"));
    assertEquals("value2", result.get("field_with_underscore"));
    assertEquals("value3", result.get("field.with.dots"));
  }

  @Test
  void testNestedObjectsWithSpecialCharacters() {
    String json = "{\"outer-field\": {\"inner_field\": {\"deep.field\": \"nested_value\"}}}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(1, result.size());
    assertEquals("nested_value", result.get("outer-field.inner_field.deep.field"));
  }

  @Test
  void testArrayWithObjectsCollectsValues() {
    String json = "{\"a\": [{\"b\": 1, \"c\": [2, 3]}, {\"b\": 4, \"c\": [5, 6, 7], \"d\": 8}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());

    // Arrays are now converted to JSON string representations
    String bValues = (String) result.get("a{}.b");
    assertEquals("[\"1\",\"4\"]", bValues);

    String cValues = (String) result.get("a{}.c{}");
    assertEquals("[\"2\",\"3\",\"5\",\"6\",\"7\"]", cValues);

    // Check single "d" value
    assertEquals("8", result.get("a{}.d"));
  }

  @Test
  void testArrayWithObjectsAndPrimitives() {
    String json =
        "{\"mixed\": [{\"name\": \"Alice\", \"age\": 25}, \"primitive\", {\"name\": \"Bob\"}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());

    // Arrays are now converted to JSON string representations
    String nameValues = (String) result.get("mixed{}.name");
    assertEquals("[\"Alice\",\"Bob\"]", nameValues);

    assertEquals("25", result.get("mixed{}.age"));
  }

  @Test
  void testNestedArraysWithObjects() {
    String json = "{\"outer\": {\"inner\": [{\"x\": 1}, {\"x\": 2, \"y\": 3}]}}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(2, result.size());

    // Arrays are now converted to JSON string representations
    String xValues = (String) result.get("outer.inner{}.x");
    assertEquals("[\"1\",\"2\"]", xValues);

    // Check single "y" value
    assertEquals("3", result.get("outer.inner{}.y"));
  }
}
