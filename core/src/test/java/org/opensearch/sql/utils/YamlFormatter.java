/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.introspect.DefaultAccessorNamingStrategy;
import tools.jackson.dataformat.yaml.YAMLFactory;
import tools.jackson.dataformat.yaml.YAMLFactoryBuilder;
import tools.jackson.dataformat.yaml.YAMLWriteFeature;

/**
 * YAML formatter utility class. Attributes are sorted alphabetically for consistent output. Check
 * {@link YamlFormatterTest} for the actual formatting behavior.
 */
public class YamlFormatter {

  private static final ObjectMapper YAML_MAPPER = initObjectMapper();

  private static ObjectMapper initObjectMapper() {
    final YAMLFactoryBuilder builder = new YAMLFactoryBuilder(new YAMLFactory());
    builder.disable(YAMLWriteFeature.WRITE_DOC_START_MARKER);
    builder.enable(YAMLWriteFeature.MINIMIZE_QUOTES); // Enable smart quoting
    builder.enable(YAMLWriteFeature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS); // Quote numeric strings
    builder.enable(YAMLWriteFeature.INDENT_ARRAYS_WITH_INDICATOR);

    ObjectMapper mapper =
        new ObjectMapper(builder.build())
            .rebuild()
            .accessorNaming(
                new DefaultAccessorNamingStrategy.Provider().withFirstCharAcceptance(true, true))
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .build();
    return mapper;
  }

  /** Formats any object into YAML. It will always use LF as line break regardless of OS. */
  public static String formatToYaml(Object object) {
    try {
      return YAML_MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(object);
    } catch (JacksonException e) {
      throw new RuntimeException("Failed to format object to YAML", e);
    }
  }
}
