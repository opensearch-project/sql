/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.minidev.json.JSONObject;
import org.junit.jupiter.api.Test;

class YamlFormatterTest {
  @Test
  void testAttributes() {
    JSONObject json1 = new JSONObject();
    json1.put("attr1", null);
    json1.put("attr2", "null");
    json1.put("attr3", 123);
    json1.put("attr4", "123");

    String actualYaml = YamlFormatter.formatToYaml(json1);

    String expectedYaml = "attr1: null\nattr2: \"null\"\nattr3: 123\nattr4: \"123\"\n";
    assertEquals(expectedYaml, actualYaml);
  }

  @Test
  void testJSONObjectConsistentOutput() {
    JSONObject json1 = new JSONObject();
    json1.put("query", "SELECT * FROM users");
    json1.put("database", "test");
    json1.put("filters", new String[] {"active = true", "role = 'user'"});

    JSONObject metadata1 = new JSONObject();
    metadata1.put("version", "1.0");
    metadata1.put("author", "system");
    json1.put("metadata", metadata1);

    // Create second JSONObject with same data but different insertion order
    JSONObject json2 = new JSONObject();
    JSONObject metadata2 = new JSONObject();
    metadata2.put("author", "system");
    metadata2.put("version", "1.0");
    json2.put("metadata", metadata2);

    json2.put("filters", new String[] {"active = true", "role = 'user'"});
    json2.put("database", "test");
    json2.put("query", "SELECT * FROM users");

    String yaml1 = YamlFormatter.formatToYaml(json1);
    String yaml2 = YamlFormatter.formatToYaml(json2);

    String expectedYaml =
        "database: test\n"
            + "filters:\n"
            + "  - active = true\n"
            + "  - role = 'user'\n"
            + "metadata:\n"
            + "  author: system\n"
            + "  version: \"1.0\"\n"
            + "query: SELECT * FROM users\n";

    assertEquals(expectedYaml, yaml1, "YAML output should match expected sorted format");
    assertEquals(yaml1, yaml2, "YAML output should be identical for same JSONObject data");
    assertTrue(yaml1.indexOf("database:") < yaml1.indexOf("filters:"));
    assertTrue(yaml1.indexOf("filters:") < yaml1.indexOf("metadata:"));
    assertTrue(yaml1.indexOf("metadata:") < yaml1.indexOf("query:"));
  }

  @Test
  void testMultiLineStrings() {
    JSONObject json = new JSONObject();
    json.put(
        "query",
        "SELECT name, age, department\n"
            + "  FROM users u\n"
            + "  JOIN departments d ON u.dept_id = d.id\n"
            + "WHERE u.active = true\n"
            + "ORDER BY u.created_date DESC");
    json.put("singleLine", "Simple single line text");
    json.put("number", 42);

    // Create nested metadata object with multi-line description
    JSONObject metadata = new JSONObject();
    metadata.put(
        "description", "Multi-line description\nof the query purpose\nand expected results");
    metadata.put("author", "system");
    metadata.put("version", "2.0");
    json.put("metadata", metadata);

    String yaml = YamlFormatter.formatToYaml(json);

    // Expected complete YAML output with nested multi-line string
    String expectedYaml =
        "metadata:\n"
            + "  author: system\n"
            + "  description: |-\n"
            + "    Multi-line description\n"
            + "    of the query purpose\n"
            + "    and expected results\n"
            + "  version: \"2.0\"\n"
            + "number: 42\n"
            + "query: |-\n"
            + "  SELECT name, age, department\n"
            + "    FROM users u\n"
            + "    JOIN departments d ON u.dept_id = d.id\n"
            + "  WHERE u.active = true\n"
            + "  ORDER BY u.created_date DESC\n"
            + "singleLine: Simple single line text\n";

    assertEquals(
        expectedYaml,
        yaml,
        "YAML output should match expected format with nested multi-line strings");
  }

  @Test
  void testFormatArbitraryObject() {
    TestObject testObj = new TestObject("test", 42);

    String yaml = YamlFormatter.formatToYaml(testObj);

    assertNotNull(yaml);
    assertTrue(yaml.contains("name:"));
    assertTrue(yaml.contains("value: 42"));
  }

  private static class TestObject {
    public String name;
    public int value;

    public TestObject(String name, int value) {
      this.name = name;
      this.value = value;
    }
  }
}
