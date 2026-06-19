/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.protocol.response.format.Format;

/**
 * Verifies {@link AnalyticsEngineFormatSupport#validateFormat(Format)} accepts the JSON/JDBC
 * default the analytics route emits and rejects unsupported output formats (csv/raw/viz) with a
 * structured {@link ErrorReport} so the REST layer returns a clean 4xx instead of silently
 * returning JSON.
 */
public class AnalyticsEngineFormatSupportTest {

  @Test
  public void jdbcFormatIsAccepted() {
    // No exception expected — JDBC is the JSON contract the analytics route emits.
    AnalyticsEngineFormatSupport.validateFormat(Format.JDBC);
  }

  @Test
  public void csvFormatIsRejectedAsUnsupportedOperation() {
    ErrorReport report =
        assertThrows(() -> AnalyticsEngineFormatSupport.validateFormat(Format.CSV));
    assertEquals(ErrorCode.UNSUPPORTED_OPERATION, report.getCode());
    assertTrue(report.getMessage().toLowerCase().contains("csv"));
    assertTrue(report.getMessage().contains("analytics engine"));
    assertNotNull(report.getSuggestion());
  }

  @Test
  public void rawFormatIsRejected() {
    ErrorReport report =
        assertThrows(() -> AnalyticsEngineFormatSupport.validateFormat(Format.RAW));
    assertEquals(ErrorCode.UNSUPPORTED_OPERATION, report.getCode());
    assertTrue(report.getMessage().toLowerCase().contains("raw"));
  }

  @Test
  public void vizFormatIsRejected() {
    ErrorReport report =
        assertThrows(() -> AnalyticsEngineFormatSupport.validateFormat(Format.VIZ));
    assertEquals(ErrorCode.UNSUPPORTED_OPERATION, report.getCode());
  }

  private static ErrorReport assertThrows(Runnable runnable) {
    try {
      runnable.run();
    } catch (ErrorReport e) {
      return e;
    }
    throw new AssertionError("Expected ErrorReport to be thrown, but nothing was thrown");
  }
}
