/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class JsonUtilsTest {

  @Test
  public void testSjsonWithSingleLine() {
    String result = JsonUtils.sjson("{'key': 'value'}");
    assertEquals("{\"key\": \"value\"}\n", result);
  }

  @Test
  public void testSjsonWithMultipleLines() {
    String result = JsonUtils.sjson("{", "  'name': 'John',", "  'age': 30", "}");
    assertEquals("{\n  \"name\": \"John\",\n  \"age\": 30\n}\n", result);
  }

  @Test
  public void testSjsonWithEmptyString() {
    String result = JsonUtils.sjson("");
    assertEquals("\n", result);
  }

  @Test
  public void testSjsonWithNoQuotes() {
    String result = JsonUtils.sjson("no quotes here");
    assertEquals("no quotes here\n", result);
  }

  @Test
  public void testSjsonWithMixedQuotes() {
    String result = JsonUtils.sjson("'single' and \"double\" quotes");
    assertEquals("\"single\" and \"double\" quotes\n", result);
  }

  @Test
  public void testSjsonWithMultipleSingleQuotes() {
    String result = JsonUtils.sjson("'key1': 'value1', 'key2': 'value2'");
    assertEquals("\"key1\": \"value1\", \"key2\": \"value2\"\n", result);
  }

  @Test
  public void testSjsonWithNestedJson() {
    String result = JsonUtils.sjson("{", "  'outer': {", "    'inner': 'value'", "  }", "}");
    assertEquals("{\n  \"outer\": {\n    \"inner\": \"value\"\n  }\n}\n", result);
  }

  @Test
  public void testSjsonWithArrays() {
    String result = JsonUtils.sjson("{", "  'items': ['item1', 'item2', 'item3']", "}");
    assertEquals("{\n  \"items\": [\"item1\", \"item2\", \"item3\"]\n}\n", result);
  }

  @Test
  public void testSjsonWithSpecialCharacters() {
    String result = JsonUtils.sjson("{'key': 'value with \\'escaped\\' quotes'}");
    assertEquals("{\"key\": \"value with \\\"escaped\\\" quotes\"}\n", result);
  }

  @Test
  public void testSjsonWithEmptyArray() {
    String result = JsonUtils.sjson();
    assertEquals("", result);
  }

  @Test
  public void testSjsonWithNullValues() {
    String result = JsonUtils.sjson("{'key': null}");
    assertEquals("{\"key\": null}\n", result);
  }

  @Test
  public void testSjsonWithNumbers() {
    String result =
        JsonUtils.sjson("{", "  'integer': 42,", "  'float': 3.14,", "  'negative': -10", "}");
    assertEquals("{\n  \"integer\": 42,\n  \"float\": 3.14,\n  \"negative\": -10\n}\n", result);
  }

  @Test
  public void testSjsonWithBooleans() {
    String result = JsonUtils.sjson("{'active': true, 'deleted': false}");
    assertEquals("{\"active\": true, \"deleted\": false}\n", result);
  }

  @Test
  public void testSjsonPreservesWhitespace() {
    String result = JsonUtils.sjson("  {'key':  'value'}  ");
    assertEquals("  {\"key\":  \"value\"}  \n", result);
  }

  @Test
  public void testSjsonWithComplexJson() {
    String result =
        JsonUtils.sjson(
            "{",
            "  'user': {",
            "    'name': 'Alice',",
            "    'email': 'alice@example.com',",
            "    'roles': ['admin', 'user'],",
            "    'active': true,",
            "    'loginCount': 42",
            "  }",
            "}");
    String expected =
        "{\n"
            + "  \"user\": {\n"
            + "    \"name\": \"Alice\",\n"
            + "    \"email\": \"alice@example.com\",\n"
            + "    \"roles\": [\"admin\", \"user\"],\n"
            + "    \"active\": true,\n"
            + "    \"loginCount\": 42\n"
            + "  }\n"
            + "}\n";
    assertEquals(expected, result);
  }
}
