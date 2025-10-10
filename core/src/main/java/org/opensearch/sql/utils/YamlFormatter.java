/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * YAML formatter utility class. Attributes are sorted alphabetically for consistent output. Check
 * {@link YamlFormatterTest} for the actual formatting behavior.
 */
public class YamlFormatter {

  private static final ObjectMapper YAML_MAPPER;

  static {
    YAMLFactory yamlFactory = new YAMLFactory();
    yamlFactory.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
    yamlFactory.disable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS);
    yamlFactory.enable(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE);
    yamlFactory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES); // Enable smart quoting
    yamlFactory.enable(
        YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS); // Quote numeric strings
    yamlFactory.enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);
    YAML_MAPPER = new ObjectMapper(yamlFactory);

    YAML_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    YAML_MAPPER.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
  }

  /**
   * Formats any object into YAML format.
   *
   * @param object the object to format
   * @return YAML-formatted string representation
   */
  public static String formatToYaml(Object object) {
    try {
      return YAML_MAPPER.writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to format object to YAML", e);
    }
  }
}
