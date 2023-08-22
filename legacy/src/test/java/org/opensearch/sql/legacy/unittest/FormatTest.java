/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.junit.Test;
import org.opensearch.sql.legacy.executor.Format;

public class FormatTest {

  @Test
  public void ofJdbcShouldReturnJDBCFormat() {
    Optional<Format> format = Format.of(Format.JDBC.getFormatName());
    assertTrue(format.isPresent());
    assertEquals(Format.JDBC, format.get());
  }

  @Test
  public void ofUnknownFormatShouldReturnEmpty() {
    assertFalse(Format.of("xml").isPresent());
  }
}
