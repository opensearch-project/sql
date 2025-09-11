/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

/**
 * YAML formatter utility class. Attributes are sorted alphabetically for consistent output. Check
 * {@link YamlFormatterTest} for the actual formatting behavior.
 */
public class YamlFormatter {

  private static final ObjectMapper YAML_MAPPER = initObjectMapper();
  private static final String LINE_BREAK_LF = "\n";
  private static final String DOUBLE_SPACE_INDENT = "  ";

  private static ObjectMapper initObjectMapper() {
    YAMLFactory yamlFactory = new YAMLFactory();
    yamlFactory.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
    yamlFactory.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES); // Enable smart quoting
    yamlFactory.enable(
        YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS); // Quote numeric strings
    yamlFactory.enable(YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR);

    ObjectMapper mapper = new ObjectMapper(yamlFactory);
    mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    mapper.setDefaultPrettyPrinter(getLfPrettyPrinter());
    return mapper;
  }

  /** Use LF as line break regardless of OS */
  private static DefaultPrettyPrinter getLfPrettyPrinter() {
    DefaultIndenter lfIndenter = new DefaultIndenter(DOUBLE_SPACE_INDENT, LINE_BREAK_LF);
    DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
    pp.indentObjectsWith(lfIndenter);
    pp.indentArraysWith(lfIndenter);
    return pp;
  }

  /** Formats any object into YAML */
  public static String formatToYaml(Object object) {
    try {
      return YAML_MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(object);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to format object to YAML", e);
    }
  }
}
