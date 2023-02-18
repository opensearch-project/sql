/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

class OptionTest {
  @Test
  public void requiredOption() {
    Optional<String> value =
        new Option.OptionBuilder()
            .name("key")
            .required(true)
            .build()
            .resolve(ImmutableMap.of("key", "value"));
    assertTrue(value.isPresent());
    assertEquals("value", value.get());
  }

  @Test
  public void requiredOptionNotExist() {
    SemanticCheckException semanticCheckException =
        assertThrows(
            SemanticCheckException.class,
            () ->
                new Option.OptionBuilder()
                    .name("key1")
                    .required(true)
                    .build()
                    .resolve(ImmutableMap.of("key", "value")));
    assertEquals("key1 is required in datasource configuration",
        semanticCheckException.getMessage());
  }

  @Test
  public void optionalOption() {
    Optional<String> value =
        new Option.OptionBuilder()
            .name("key")
            .required(false)
            .build()
            .resolve(ImmutableMap.of("key", "value"));
    assertTrue(value.isPresent());
    assertEquals("value", value.get());
  }

  @Test
  public void optionalOptionNotExist() {
    Optional<String> value =
        new Option.OptionBuilder()
            .name("key1")
            .required(false)
            .build()
            .resolve(ImmutableMap.of("key", "value"));
    assertFalse(value.isPresent());
  }
}
