/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class JsonExtractAllFunctionImplTest {

  private final JsonExtractAllFunctionImpl function = new JsonExtractAllFunctionImpl();

  @SuppressWarnings("unchecked")
  private Map<String, String> jsonExtractAll(String json) {
    Object result = JsonExtractAllFunctionImpl.eval(json);
    if (result == null) {
      return null;
    }
    return (Map<String, String>) result;
  }

  @Test
  public void testReturnTypeInference() {
    assertNotNull(function.getReturnTypeInference(), "Return type inference should not be null");
  }

  @Test
  public void testOperandMetadata() {
    assertNotNull(function.getOperandMetadata(), "Operand metadata should not be null");
  }

  @Test
  public void testFunctionConstructor() {
    assertNotNull(new JsonExtractAllFunctionImpl(), "Function should be properly initialized");
  }

  @Test
  public void testNoArguments() {
    assertNull(JsonExtractAllFunctionImpl.eval());
  }

  @Test
  public void testNullInput() {
    assertNull(jsonExtractAll(null));
  }

  @Test
  public void testEmptyString() {
    assertNull(jsonExtractAll(""));
  }

  @Test
  public void testWhitespaceString() {
    assertNull(jsonExtractAll("   "));
  }

  @Test
  public void testEmptyJsonObject() {
    assertThat(jsonExtractAll("{}"), anEmptyMap());
  }

  @Test
  public void testSimpleJsonObject() {
    assertThat(
        jsonExtractAll(
            """
            {
              "name": "John",
              "age": 30
            }\
            """),
        is(Map.of("name", "John", "age", "30")));
  }

  @Test
  public void testInvalidJsonReturnResults() {
    assertThat(jsonExtractAll("{\"name\": \"John\", \"age\":}"), is(Map.of("name", "John")));
  }

  @Test
  public void testNonObjectJsonArray() {
    assertThat(jsonExtractAll("[1, 2, 3]"), is(Map.of("{}", "[1, 2, 3]")));
  }

  @Test
  public void testTopLevelArrayOfObjects() {
    assertThat(
        jsonExtractAll(
            """
            [
              {"age": 1},
              {"age": 2}
            ]\
            """),
        is(Map.of("{}.age", "[1, 2]")));
  }

  @Test
  public void testTopLevelArrayOfComplexObjects() {
    assertThat(
        jsonExtractAll(
            """
            [
              {"name": "John", "age": 30},
              {"name": "Jane", "age": 25}
            ]\
            """),
        is(Map.of("{}.name", "[John, Jane]", "{}.age", "[30, 25]")));
  }

  @Test
  public void testNonObjectJsonPrimitive() {
    assertNull(jsonExtractAll("\"just a string\""));
  }

  @Test
  public void testNonObjectJsonNumber() {
    assertNull(jsonExtractAll("42"));
  }

  @Test
  public void testSingleLevelNesting() {
    assertThat(
        jsonExtractAll(
            """
            {
              "user": {"name": "John"},
              "system": "linux"
            }\
            """),
        is(Map.of("user.name", "John", "system", "linux")));
  }

  @Test
  public void testMultiLevelNesting() {
    assertThat(
        jsonExtractAll(
            """
            {
              "a": {
                "b": {
                  "c": "value"
                }
              }
            }\
            """),
        is(Map.of("a.b.c", "value")));
  }

  @Test
  public void testMixedNestedAndFlat() {
    assertThat(
        jsonExtractAll(
            """
            {
              "name": "John",
              "address": {
                "city": "NYC",
                "zip": "10001"
              }
            }\
            """),
        is(Map.of("name", "John", "address.city", "NYC", "address.zip", "10001")));
  }

  @Test
  public void testDeeplyNestedStructure() {
    assertThat(
        jsonExtractAll(
            """
            {
              "level1": {
                "level2": {
                  "level3": {
                    "level4": {
                      "level5": "deep"
                    }
                  }
                }
              }
            }\
            """),
        is(Map.of("level1.level2.level3.level4.level5", "deep")));
  }

  @Test
  public void testSimpleArray() {
    assertThat(
        jsonExtractAll(
            """
            {
              "tags": ["a", "b", "c"]
            }\
            """),
        is(Map.of("tags{}", "[a, b, c]")));
  }

  @Test
  public void testArrayOfObjects() {
    assertThat(
        jsonExtractAll(
            """
            {
              "users": [
                {"name": "John"},
                {"name": "Jane"}
              ]
            }\
            """),
        is(Map.of("users{}.name", "[John, Jane]")));
  }

  @Test
  public void testNestedArray() {
    assertThat(
        jsonExtractAll(
            """
            {
              "data": {
                "items": [1, 2, 3]
              }
            }\
            """),
        is(Map.of("data.items{}", "[1, 2, 3]")));
  }

  @Test
  public void testNested() {
    assertThat(
        jsonExtractAll(
            """
            {
              "data": {
                "items": [[1, 2, {"hello": 3}], 4],
                "other": 5
              },
              "another": [6, [7, 8], 9]
            }\
            """),
        is(
            Map.of(
                "data.items{}{}", "[1, 2]",
                "data.items{}{}.hello", "3",
                "data.items{}", "4",
                "data.other", "5",
                "another{}", "[6, 9]",
                "another{}{}", "[7, 8]")));
  }

  @Test
  public void testEmptyArray() {
    assertNull(jsonExtractAll("{\"empty\": []}").get("empty{}"));
  }

  @Test
  public void testStringValues() {
    assertThat(
        jsonExtractAll(
            """
            {
              "text": "hello world",
              "empty": ""
            }\
            """),
        is(Map.of("text", "hello world", "empty", "")));
  }

  @Test
  public void testNumericValues() {
    assertThat(
        jsonExtractAll(
            """
            {
              "int": 42,
              "long": 9223372036854775807,
              "hugeNumber": 9223372036854775808,
              "double": 3.14159
            }\
            """),
        is(
            Map.of(
                "int", "42",
                "long", "9223372036854775807",
                "hugeNumber", "9.223372036854776E18",
                "double", "3.14159")));
  }

  @Test
  public void testBooleanValues() {
    assertThat(
        jsonExtractAll(
            """
            {
              "isTrue": true,
              "isFalse": false
            }\
            """),
        is(Map.of("isTrue", "true", "isFalse", "false")));
  }

  @Test
  public void testNullValues() {
    assertThat(
        jsonExtractAll(
            """
            {
              "nullValue": null,
              "notNull": "value"
            }\
            """),
        is(Map.of("nullValue", "null", "notNull", "value")));
  }

  @Test
  public void testNullValuesInArray() {
    assertThat(
        jsonExtractAll(
            """
            [
              {"a": null},
              {"a": 1}
            ]\
            """),
        is(Map.of("{}.a", "[null, 1]")));
  }

  @Test
  public void testMixedTypesInArray() {
    assertThat(
        jsonExtractAll(
            """
            {
              "mixed": ["string", 42, true, null, 3.14]
            }\
            """),
        is(Map.of("mixed{}", "[string, 42, true, null, 3.14]")));
  }

  @Test
  public void testSpecialCharactersInKeys() {
    assertThat(
        jsonExtractAll(
            """
            {
              "key.with.dots": "value1",
              "key-with-dashes": "value2",
              "key_with_underscores": "value3"
            }\
            """),
        is(
            Map.of(
                "key.with.dots", "value1",
                "key-with-dashes", "value2",
                "key_with_underscores", "value3")));
  }

  @Test
  public void testUnicodeCharacters() {
    assertThat(
        jsonExtractAll(
            """
            {
              "unicode": "こんにちは",
              "emoji": "🚀",
              "🚀": 1
            }\
            """),
        is(Map.of("unicode", "こんにちは", "emoji", "🚀", "🚀", "1")));
  }

  @Test
  public void testComplexNestedStructure() {
    assertThat(
        jsonExtractAll(
            """
            {
              "user": {
                "profile": {
                  "name": "John",
                  "contacts": [
                    {"type": "email", "value": "john@example.com"},
                    {"type": "phone", "value": "123-456-7890"}
                  ]
                },
                "preferences": {
                  "theme": "dark",
                  "notifications": true
                }
              }
            }\
            """),
        is(
            Map.of(
                "user.profile.name", "John",
                "user.profile.contacts{}.type", "[email, phone]",
                "user.profile.contacts{}.value", "[john@example.com, 123-456-7890]",
                "user.preferences.theme", "dark",
                "user.preferences.notifications", "true")));
  }

  @Test
  public void testLargeJsonObject() {
    StringBuilder jsonBuilder = new StringBuilder("{");
    for (int i = 0; i < 100; i++) {
      if (i > 0) jsonBuilder.append(",");
      jsonBuilder.append("\"field").append(i).append("\": ").append(i);
    }
    jsonBuilder.append("}");

    Map<String, String> map = jsonExtractAll(jsonBuilder.toString());
    assertEquals(100, map.size());
    assertEquals("0", map.get("field0"));
    assertEquals("99", map.get("field99"));
  }
}
