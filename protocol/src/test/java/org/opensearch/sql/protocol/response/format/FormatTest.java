/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Unit test for {@link Format}. */
public class FormatTest {

  @Test
  void defaultFormat() {
    Optional<Format> format = Format.of("");
    assertTrue(format.isPresent());
    assertEquals(Format.JDBC, format.get());
  }

  @Test
  void jdbc() {
    Optional<Format> format = Format.of("jdbc");
    assertTrue(format.isPresent());
    assertEquals(Format.JDBC, format.get());
  }

  @Test
  void csv() {
    Optional<Format> format = Format.of("csv");
    assertTrue(format.isPresent());
    assertEquals(Format.CSV, format.get());
  }

  @Test
  void raw() {
    Optional<Format> format = Format.of("raw");
    assertTrue(format.isPresent());
    assertEquals(Format.RAW, format.get());
  }

  @Test
  void caseSensitive() {
    Optional<Format> format = Format.of("JDBC");
    assertTrue(format.isPresent());
    assertEquals(Format.JDBC, format.get());
  }

  @Test
  void unsupportedFormat() {
    Optional<Format> format = Format.of("notsupport");
    assertFalse(format.isPresent());
  }
}
