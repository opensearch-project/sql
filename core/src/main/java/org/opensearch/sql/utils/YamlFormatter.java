/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.introspect.DefaultAccessorNamingStrategy;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLFactoryBuilder;
import tools.jackson.dataformat.yaml.YAMLWriteFeature;

/**
 * YAML formatter utility class. Attributes are sorted alphabetically for consistent output. Check
 * {@link YamlFormatterTest} for the actual formatting behavior.
 */
public class YamlFormatter {

  private static final ObjectMapper YAML_MAPPER;

  static {
    final YAMLFactoryBuilder builder = new YAMLFactoryBuilder(new YAMLFactory());
    builder.disable(YAMLWriteFeature.WRITE_DOC_START_MARKER);
    builder.enable(YAMLWriteFeature.LITERAL_BLOCK_STYLE);
    builder.enable(YAMLWriteFeature.MINIMIZE_QUOTES); // Enable smart quoting
    builder.enable(YAMLWriteFeature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS); // Quote numeric strings
    builder.enable(YAMLWriteFeature.INDENT_ARRAYS_WITH_INDICATOR);

    YAML_MAPPER =
        new ObjectMapper(builder.build())
            .rebuild()
            .accessorNaming(
                new DefaultAccessorNamingStrategy.Provider().withFirstCharAcceptance(true, true))
            .changeDefaultPropertyInclusion(
                incl -> incl.withValueInclusion(JsonInclude.Include.NON_NULL))
            .changeDefaultPropertyInclusion(
                incl -> incl.withContentInclusion(JsonInclude.Include.NON_NULL))
            .build();
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
    } catch (JacksonException e) {
      throw new RuntimeException("Failed to format object to YAML", e);
    }
  }
}
