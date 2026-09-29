/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import java.util.Locale;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.protocol.response.format.Format;

/**
 * Guards the analytics-engine query route, which only produces JSON. Rejects the alternate output
 * formats (csv/raw/viz) the route does not implement, instead of silently answering with JSON.
 */
public final class AnalyticsEngineFormatSupport {

  private AnalyticsEngineFormatSupport() {}

  /**
   * Throw an {@link ErrorReport} if the requested output format is unsupported on the analytics
   * engine. JSON/JDBC (the default) is supported; csv/raw/viz are not.
   */
  public static void validateFormat(Format format) {
    // JDBC (the default) is the JSON contract the analytics route emits.
    if (format == Format.JDBC) {
      return;
    }
    throw ErrorReport.wrap(
            new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "response in %s format is not supported on the analytics engine",
                    format.getFormatName())))
        .code(ErrorCode.UNSUPPORTED_OPERATION)
        .stage(QueryProcessingStage.ANALYZING)
        .location("while selecting the response format for the analytics engine")
        .suggestion(
            "The analytics engine only returns JSON. Remove the 'format' parameter, or run this"
                + " query against a non-analytics index to use the requested format.")
        .build();
  }
}
