/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class JsonToYamlConverterTest {

  @Test
  void testConvertSimpleJsonToYaml() {
    // Test that JSON strings are properly parsed and converted to YAML
    String jsonString =
        "{\"query\": \"SELECT * FROM users\", \"database\": \"test\", \"version\": 1.0}";

    String yaml = JsonToYamlConverter.convertJsonToYaml(jsonString);

    String expectedYaml = "database: test\nquery: SELECT * FROM users\nversion: 1.0\n";
    assertEquals(expectedYaml, yaml);
  }

  @Test
  void testConvertComplexJsonToYaml() {
    // Test complex nested JSON string conversion
    String jsonString =
        "{\"explain\": {\"logical_plan\": \"LogicalProject\", \"physical_plan\":"
            + " \"PhysicalProject\"}, \"status\": \"success\"}";

    String yaml = JsonToYamlConverter.convertJsonToYaml(jsonString);

    // Verify it's properly structured YAML, not a string literal
    assertTrue(yaml.contains("explain:"));
    assertTrue(yaml.contains("logical_plan: LogicalProject"));
    assertTrue(yaml.contains("physical_plan: PhysicalProject"));
    assertTrue(yaml.contains("status: success"));
    // Should be sorted alphabetically
    assertTrue(yaml.indexOf("explain:") < yaml.indexOf("status:"));
  }

  @Test
  void testConvertJsonArrayToYaml() {
    // Test JSON array conversion
    String jsonString = "{\"items\": [\"item1\", \"item2\", \"item3\"], \"count\": 3}";

    String yaml = JsonToYamlConverter.convertJsonToYaml(jsonString);

    assertTrue(yaml.contains("count: 3"));
    assertTrue(yaml.contains("items:"));
    assertTrue(yaml.contains("- item1"));
    assertTrue(yaml.contains("- item2"));
    assertTrue(yaml.contains("- item3"));
  }

  @Test
  void testConvertInvalidJsonThrowsException() {
    // Test that invalid JSON strings throw an exception
    String invalidJson = "This is not JSON";

    RuntimeException exception =
        assertThrows(
            RuntimeException.class, () -> JsonToYamlConverter.convertJsonToYaml(invalidJson));

    assertTrue(exception.getMessage().contains("Failed to parse JSON string"));
  }

  @Test
  void testConvertEmptyJsonObject() {
    // Test empty JSON object
    String jsonString = "{}";

    String yaml = JsonToYamlConverter.convertJsonToYaml(jsonString);

    assertEquals("{}\n", yaml);
  }

  @Test
  void testConvertJsonWithNullValues() {
    // Test JSON with null values
    String jsonString = "{\"name\": \"John\", \"age\": null, \"active\": true}";

    String yaml = JsonToYamlConverter.convertJsonToYaml(jsonString);

    assertTrue(yaml.contains("active: true"));
    assertTrue(yaml.contains("age: null"));
    assertTrue(yaml.contains("name: John"));
  }
}
