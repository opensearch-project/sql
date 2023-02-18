/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.parser;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.sql.exception.SemanticCheckException;

/** Describe a single datasource configuration option. */
@Builder
public class Option {
  /** Configuration name. */
  @Getter private final String name;

  /** It the configuration required. */
  private final boolean required;

  /**
   * Resolve the datasource property.
   *
   * @param properties properties.
   * @return optional value.
   */
  public Optional<String> resolve(Map<String, String> properties) {
    if (required && !properties.containsKey(name)) {
      throw new SemanticCheckException(
          String.format(Locale.ROOT, "%s is required in datasource configuration", name));
    } else {
      return Optional.ofNullable(properties.get(name));
    }
  }
}
