/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Utility class for converting JSON string to YAML string. */
public class JsonToYamlConverter {

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  public static String convertJsonToYaml(String jsonString) {
    try {
      // Parse the JSON string into an object
      Object parsedJson = JSON_MAPPER.readValue(jsonString, Object.class);

      // Use YamlFormatter to convert the parsed object to YAML
      return YamlFormatter.formatToYaml(parsedJson);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse JSON string: " + jsonString, e);
    }
  }
}
