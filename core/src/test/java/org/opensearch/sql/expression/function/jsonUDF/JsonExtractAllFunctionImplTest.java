/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
    assertEquals(30, result.get("age"));
    assertEquals(true, result.get("active"));
  }

  @Test
  void testNestedObjectExtraction() {
    String json =
        "{\"user\": {\"name\": \"Alice\", \"profile\": {\"age\": 28, \"location\": \"Seattle\"}}}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());
    assertEquals("Alice", result.get("user.name"));
    assertEquals(28, result.get("user.profile.age"));
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
    assertEquals(1, result.get("id"));
    assertEquals("Bob", result.get("user.name"));
    assertEquals(35, result.get("user.details.age"));
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

    @SuppressWarnings("unchecked")
    List<Object> hobbies = (List<Object>) result.get("hobbies{}");
    assertEquals(3, hobbies.size());
    assertEquals("reading", hobbies.get(0));
    assertEquals("swimming", hobbies.get(1));
    assertEquals("coding", hobbies.get(2));
  }

  @Test
  void testNestedArrayWithObjects() {
    String json =
        "{\"users\": [{\"name\": \"Alice\", \"age\": 25}, {\"name\": \"Bob\", \"age\": 30}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(1, result.size());

    @SuppressWarnings("unchecked")
    List<Object> users = (List<Object>) result.get("users{}");
    assertEquals(2, users.size());

    // Array elements that are objects should be preserved as maps
    @SuppressWarnings("unchecked")
    Map<String, Object> user1 = (Map<String, Object>) users.get(0);
    assertEquals("Alice", user1.get("name"));
    assertEquals(25, user1.get("age"));

    @SuppressWarnings("unchecked")
    Map<String, Object> user2 = (Map<String, Object>) users.get(1);
    assertEquals("Bob", user2.get("name"));
    assertEquals(30, user2.get("age"));
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
    assertEquals(42, result.get("integer"));
    assertEquals(3.14, result.get("double"));
    assertEquals(false, result.get("boolean"));
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
    assertEquals(123, result.get("user.id"));
    assertEquals("John Doe", result.get("user.profile.name"));
    assertEquals("john@example.com", result.get("user.profile.email"));
    assertEquals("dark", result.get("user.profile.preferences.theme"));
    assertEquals(true, result.get("user.profile.preferences.notifications"));
    assertEquals("2023-01-01", result.get("metadata.created"));
    assertEquals(1.2, result.get("metadata.version"));

    @SuppressWarnings("unchecked")
    List<Object> roles = (List<Object>) result.get("user.roles{}");
    assertEquals(2, roles.size());
    assertEquals("admin", roles.get(0));
    assertEquals("user", roles.get(1));
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
    assertEquals(42, result.get("value"));
    // Empty nested objects should not contribute any fields
  }

  @Test
  void testArrayWithMixedTypes() {
    String json = "{\"mixed\": [\"string\", 42, true, null, {\"nested\": \"object\"}]}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(1, result.size());

    @SuppressWarnings("unchecked")
    List<Object> mixed = (List<Object>) result.get("mixed{}");
    assertEquals(5, mixed.size());
    assertEquals("string", mixed.get(0));
    assertEquals(42, mixed.get(1));
    assertEquals(true, mixed.get(2));
    assertNull(mixed.get(3));

    @SuppressWarnings("unchecked")
    Map<String, Object> nestedObj = (Map<String, Object>) mixed.get(4);
    assertEquals("object", nestedObj.get("nested"));
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
  void testLargeNumbers() {
    String json =
        "{\"small_int\": 42, \"large_long\": 9223372036854775807, \"big_double\":"
            + " 1.7976931348623157E308}";

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) JsonExtractAllFunctionImpl.eval(json);

    assertEquals(3, result.size());
    assertEquals(42, result.get("small_int"));
    assertEquals(9223372036854775807L, result.get("large_long"));
    assertEquals(1.7976931348623157E308, result.get("big_double"));
  }
}
