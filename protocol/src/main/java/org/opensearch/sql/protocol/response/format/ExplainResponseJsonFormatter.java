/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import java.util.LinkedHashMap;
import java.util.Map;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;

/**
 * Renders an {@link ExplainResponse} to the JSON shape the synchronous {@code POST /_plugins/_ppl}
 * explain path produces. When the response carries a Calcite tree, emit {@code {"calcite":
 * {"logical": ..., "physical": ...}}}; otherwise the response is serialized as-is.
 */
public final class ExplainResponseJsonFormatter extends JsonResponseFormatter<ExplainResponse> {

  public ExplainResponseJsonFormatter(Style style) {
    super(style);
  }

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
}
