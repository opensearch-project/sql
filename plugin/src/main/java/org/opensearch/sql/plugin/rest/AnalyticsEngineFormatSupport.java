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
 * Guards the analytics-engine (Calcite/DataFusion) query route, which only produces JSON output.
 *
 * <p>The unified analytics route formats every response with {@code SimpleJsonResponseFormatter}
 * and does not implement the alternate response formats (csv/raw/viz) that the classic SQL/PPL
 * engines support. Previously a {@code format=csv} request on an analytics index was silently
 * answered with JSON, which is misleading: the client asked for one content type and received
 * another with no indication the parameter was dropped.
 *
 * <p>This guard rejects an unsupported output format up front with a structured {@link ErrorReport}
 * coded {@link ErrorCode#UNSUPPORTED_OPERATION}, so the REST layer returns a clean client error
 * (4xx) with an actionable message instead of silently ignoring the request.
 */
public final class AnalyticsEngineFormatSupport {

  private AnalyticsEngineFormatSupport() {}

  /**
   * Throw an {@link ErrorReport} if the requested output format is not supported on the analytics
   * engine. JSON/JDBC (the default) is supported; csv/raw/viz are not.
   *
   * @param format the resolved response format requested by the client
   * @throws ErrorReport (coded {@link ErrorCode#UNSUPPORTED_OPERATION}) when the format is
   *     unsupported on the analytics route
   */
  public static void validateFormat(Format format) {
    // JDBC (the default) is the JSON contract the analytics route emits. Anything else is an
    // alternate output format that the analytics route does not implement.
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
