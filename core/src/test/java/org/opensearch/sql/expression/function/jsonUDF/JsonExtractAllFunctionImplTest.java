/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JsonExtractAllFunctionImplTest {

  private final JsonExtractAllFunctionImpl function = new JsonExtractAllFunctionImpl();

  @SuppressWarnings("unchecked")
  private Map<String, Object> assertValidMapResult(Object result) {
    assertNotNull(result);
    assertTrue(result instanceof Map);
    return (Map<String, Object>) result;
  }

  @SuppressWarnings("unchecked")
  private List<Object> assertListValue(Map<String, Object> map, String key) {
    Object value = map.get(key);
    assertNotNull(value);
    assertTrue(value instanceof List);
    return (List<Object>) value;
  }

  private void assertListEquals(List<Object> actual, Object... expected) {
    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual.get(i));
    }
  }

  private void assertMapListValue(Map<String, Object> map, String key, Object... expectedValues) {
    List<Object> list = assertListValue(map, key);
    assertListEquals(list, expectedValues);
  }

  private void assertMapValue(Map<String, Object> map, String key, Object expectedValue) {
    assertEquals(expectedValue, map.get(key));
  }

  private Map<String, Object> eval(String json) {
    Object result = JsonExtractAllFunctionImpl.eval(json);
    return assertValidMapResult(result);
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
    JsonExtractAllFunctionImpl testFunction = new JsonExtractAllFunctionImpl();

    assertNotNull(testFunction, "Function should be properly initialized");
  }

  private void assertEvalNull(Object... args) {
    assertNull(JsonExtractAllFunctionImpl.eval(args));
  }

  @Test
  public void testNoArguments() {
    Object result = JsonExtractAllFunctionImpl.eval();

    assertNull(result);
  }

  @Test
  public void testNullInput() {}

  @Test
  public void testEmptyString() {
    assertEvalNull();
    assertEvalNull((String) null);
    assertEvalNull("");
    assertEvalNull("", "");
    assertEvalNull("  ");
    assertEvalNull("{}");
    assertEvalNull("\"just a string\"");
    assertEvalNull("123");
  }

  @Test
  public void testSimpleJsonObject() throws Exception {
    Map<String, Object> map = eval("{\"name\": \"John\", \"age\": 30}");

    assertEquals("John", map.get("name"));
    assertEquals(30, map.get("age"));
    assertEquals(2, map.size());
  }

  @Test
  public void testInvalidJsonReturnResults() {
    Map<String, Object> map = eval("{\"name\": \"John\", \"age\":}");

    assertEquals("John", map.get("name"));
    assertEquals(1, map.size());
  }

  @Test
  public void testNonObjectJsonArray() {
    Map<String, Object> map = eval("[1, 2, 3]");

    assertMapListValue(map, "{}", 1, 2, 3);
    assertEquals(1, map.size());
  }

  @Test
  public void testTopLevelArrayOfObjects() {
    Map<String, Object> map = eval("[{\"age\": 1}, {\"age\": 2}]");

    assertMapListValue(map, "{}.age", 1, 2);
    assertEquals(1, map.size());
  }

  @Test
  public void testTopLevelArrayOfComplexObjects() {
    Map<String, Object> map =
        eval("[{\"name\": \"John\", \"age\": 30}, {\"name\": \"Jane\", \"age\": 25}]");

    assertMapListValue(map, "{}.name", "John", "Jane");
    assertMapListValue(map, "{}.age", 30, 25);
    assertEquals(2, map.size());
  }

  @Test
  public void testSingleLevelNesting() {
    Map<String, Object> map = eval("{\"user\": {\"name\": \"John\"}, \"system\": \"linux\"}");

    assertEquals("John", map.get("user.name"));
    assertEquals("linux", map.get("system"));
    assertEquals(2, map.size());
  }

  @Test
  public void testMultiLevelNesting() {
    Map<String, Object> map = eval("{\"a\": {\"b\": {\"c\": \"value\"}}}");

    assertEquals("value", map.get("a.b.c"));
    assertEquals(1, map.size());
  }

  @Test
  public void testMixedNestedAndFlat() {
    Map<String, Object> map =
        eval("{\"name\": \"John\", \"address\": {\"city\": \"NYC\", \"zip\": \"10001\"}}");

    assertEquals("John", map.get("name"));
    assertEquals("NYC", map.get("address.city"));
    assertEquals("10001", map.get("address.zip"));
    assertEquals(3, map.size());
  }

  @Test
  public void testDeeplyNestedStructure() {
    Map<String, Object> map =
        eval("{\"level1\": {\"level2\": {\"level3\": {\"level4\": {\"level5\": \"deep\"}}}}}");

    assertEquals("deep", map.get("level1.level2.level3.level4.level5"));
    assertEquals(1, map.size());
  }

  @Test
  public void testSimpleArray() {
    Map<String, Object> map = eval("{\"tags\": [\"a\", \"b\", \"c\"]}");

    assertMapListValue(map, "tags{}", "a", "b", "c");
    assertEquals(1, map.size());
  }

  @Test
  public void testArrayOfObjects() {
    Map<String, Object> map = eval("{\"users\": [{\"name\": \"John\"}, {\"name\": \"Jane\"}]}");

    assertMapListValue(map, "users{}.name", "John", "Jane");
    assertEquals(1, map.size());
  }

  @Test
  public void testNestedArray() {
    Map<String, Object> map = eval("{\"data\": {\"items\": [1, 2, 3]}}");

    assertMapListValue(map, "data.items{}", 1, 2, 3);
    assertEquals(1, map.size());
  }

  @Test
  public void testNested() {
    Map<String, Object> map =
        eval(
            "{\"data\": {\"items\": [[1, 2, {\"hello\": 3}], 4], \"other\": 5}, \"another\": [6,"
                + " [7, 8], 9]}");

    assertMapListValue(map, "data.items{}{}", 1, 2);
    assertMapValue(map, "data.items{}{}.hello", 3);
    assertMapValue(map, "data.items{}", 4);
    assertMapValue(map, "data.other", 5);
    assertMapListValue(map, "another{}", 6, 9);
    assertMapListValue(map, "another{}{}", 7, 8);
    assertEquals(6, map.size());
  }

  @Test
  public void testEmptyArray() {
    assertEvalNull("{\"empty\": []}");
  }

  @Test
  public void testStringValues() {
    Map<String, Object> map = eval("{\"text\": \"hello world\", \"empty\": \"\"}");

    assertMapValue(map, "text", "hello world");
    assertMapValue(map, "empty", "");
    assertEquals(2, map.size());
  }

  @Test
  public void testNumericValues() {
    Map<String, Object> map =
        eval(
            "{\"int\": 42, \"long\": 9223372036854775807, \"hugeNumber\": 9223372036854775808,"
                + " \"double\": 3.14159}");

    assertEquals(4, map.size());
    assertEquals(42, map.get("int"));
    assertEquals(9223372036854775807L, map.get("long"));
    assertEquals(9223372036854775808.0, map.get("hugeNumber"));
    assertEquals(3.14159, map.get("double"));
  }

  @Test
  public void testBooleanValues() {
    Map<String, Object> map = eval("{\"isTrue\": true, \"isFalse\": false}");

    assertEquals(true, map.get("isTrue"));
    assertEquals(false, map.get("isFalse"));
    assertEquals(2, map.size());
  }

  @Test
  public void testNullValues() {
    Map<String, Object> map = eval("{\"nullValue\": null, \"notNull\": \"value\"}");

    assertNull(map.get("nullValue"));
    assertEquals("value", map.get("notNull"));
    assertEquals(2, map.size());
  }

  @Test
  public void testMixedTypesInArray() {
    Map<String, Object> map = eval("{\"mixed\": [\"string\", 42, true, null, 3.14]}");

    List<Object> mixed = (List<Object>) assertListValue(map, "mixed{}");
    assertEquals(5, mixed.size());
    assertEquals("string", mixed.get(0));
    assertEquals(42, mixed.get(1));
    assertEquals(true, mixed.get(2));
    assertNull(mixed.get(3));
    assertEquals(3.14, mixed.get(4));
    assertEquals(1, map.size());
  }

  @Test
  public void testSpecialCharactersInKeys() {
    Map<String, Object> map =
        eval(
            "{\"key.with.dots\": \"value1\", \"key-with-dashes\": \"value2\","
                + " \"key_with_underscores\": \"value3\"}");

    assertEquals("value1", map.get("key.with.dots"));
    assertEquals("value2", map.get("key-with-dashes"));
    assertEquals("value3", map.get("key_with_underscores"));
    assertEquals(3, map.size());
  }

  @Test
  public void testUnicodeCharacters() {
    Map<String, Object> map = eval("{\"unicode\": \"こんにちは\", \"emoji\": \"🚀\", \"🚀\": 1}");

    assertEquals("こんにちは", map.get("unicode"));
    assertEquals("🚀", map.get("emoji"));
    assertEquals(1, map.get("🚀"));
    assertEquals(3, map.size());
  }

  @Test
  public void testComplexNestedStructure() {
    Map<String, Object> map =
        eval(
            "{\"user\": {\"profile\": {\"name\": \"John\", \"contacts\": [{\"type\": \"email\","
                + " \"value\": \"john@example.com\"}, {\"type\": \"phone\", \"value\":"
                + " \"123-456-7890\"}]}, \"preferences\": {\"theme\": \"dark\", \"notifications\":"
                + " true}}}");

    assertEquals("John", map.get("user.profile.name"));
    assertMapListValue(map, "user.profile.contacts{}.type", "email", "phone");
    assertMapListValue(map, "user.profile.contacts{}.value", "john@example.com", "123-456-7890");
    assertEquals("dark", map.get("user.preferences.theme"));
    assertEquals(true, map.get("user.preferences.notifications"));
    assertEquals(5, map.size());
  }

  @Test
  public void testLargeJsonObject() {
    StringBuilder jsonBuilder = new StringBuilder("{");
    for (int i = 0; i < 100; i++) {
      if (i > 0) jsonBuilder.append(",");
      jsonBuilder.append("\"field").append(i).append("\": ").append(i);
    }
    jsonBuilder.append("}");

    Map<String, Object> map = eval(jsonBuilder.toString());
    assertEquals(100, map.size());
    assertEquals(0, map.get("field0"));
    assertEquals(99, map.get("field99"));
  }

  @Test
  public void testArrayInputWithSingleJsonObject() {
    List<String> array = Arrays.asList("{\"name\": \"John\", \"age\": 30}");
    Object result = JsonExtractAllFunctionImpl.eval(array);
    Map<String, Object> map = assertValidMapResult(result);

    assertEquals("John", map.get("name"));
    assertEquals(30, map.get("age"));
    assertEquals(2, map.size());
  }

  @Test
  public void testArrayInputWithMultipleJsonFragments() {
    List<String> array = Arrays.asList("{\"name\": \"John\"", ", \"age\": 30}");
    Object result = JsonExtractAllFunctionImpl.eval(array);
    Map<String, Object> map = assertValidMapResult(result);

    assertEquals("John", map.get("name"));
    assertEquals(30, map.get("age"));
    assertEquals(2, map.size());
  }

  @Test
  public void testArrayInputWithNullElements() {
    List<String> array = Arrays.asList("{\"name\": ", null, "\"John\", \"age\": 30}");
    Object result = JsonExtractAllFunctionImpl.eval(array);
    Map<String, Object> map = assertValidMapResult(result);

    assertEquals("John", map.get("name"));
    assertEquals(30, map.get("age"));
    assertEquals(2, map.size());
  }

  @Test
  public void testNullAndEmptyArray() {
    assertEvalNull(Arrays.asList(null, null, null));
    assertEvalNull(Arrays.asList());
    assertEvalNull((List<?>) null);
  }
}
