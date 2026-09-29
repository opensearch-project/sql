/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.LinkedHashMap;
import java.util.Map;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

/**
 * Serializes an {@link ExplainResponse} to the same JSON shape the synchronous {@code POST
 * /_plugins/_ppl} explain path produces. Extracted so both the async-query core (owner-side
 * toAsyncResponse) and the plugin transport action (sync-window response) render explain output
 * identically.
 */
public final class ExplainResponseJsonFormatter {

  private ExplainResponseJsonFormatter() {}

  /**
   * @param response engine-produced explain payload
   * @return pretty-printed JSON string matching the sync-path shape (calcite {logical, physical} or
   *     legacy {root} depending on which fields the response carries)
   */
  public static String format(ExplainResponse response) {
    JsonResponseFormatter<ExplainResponse> formatter =
        new JsonResponseFormatter<>(JsonResponseFormatter.Style.PRETTY) {
          @Override
          protected Object buildJsonObject(ExplainResponse response) {
            if (response.getCalcite() != null && response.getCalcite().getLogicalTree() != null) {
              Map<String, Object> result = new LinkedHashMap<>();
              Map<String, Object> calcite = new LinkedHashMap<>();
              calcite.put("logical", response.getCalcite().getLogicalTree());
              if (response.getCalcite().getPhysicalTree() != null) {
                calcite.put("physical", response.getCalcite().getPhysicalTree());
              }
              result.put("calcite", calcite);
              return result;
            }
            return response;
          }
        };
    return formatter.format(response);
  }
}
