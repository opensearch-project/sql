/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.parser;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * JDBC datasource properties parser.
 */
public class PropertiesParser {

  public static final String URL = "url";

  public static final String USERNAME = "username";

  public static final String PASSWORD = "password";

  public static final String DRIVER = "driver";

  private final List<Option> options;

  /**
   * Constructor.
   */
  public PropertiesParser() {
    options =
        new ImmutableList.Builder<Option>()
            .add(Option.builder().name(URL).required(true).build())
            .add(Option.builder().name(DRIVER).required(true).build())
            .add(Option.builder().name(USERNAME).required(false).build())
            .add(Option.builder().name(PASSWORD).required(false).build())
            .build();
  }

  /**
   * Parse datasource properties.
   * @param properties datasource properties.
   * @return {@link Properties}.
   */
  public Properties parse(Map<String, String> properties) {
    Properties result = new Properties();

    for (Option option : options) {
      Optional<String> value = option.resolve(properties);
      value.map(v -> result.put(option.getName(), value.get()));
    }
    return result;
  }
}
