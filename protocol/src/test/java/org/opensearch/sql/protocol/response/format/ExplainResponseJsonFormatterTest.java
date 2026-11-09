/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.COMPACT;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNodeV2;

class ExplainResponseJsonFormatterTest {

  private final ExplainResponseJsonFormatter formatter = new ExplainResponseJsonFormatter(COMPACT);

  @Test
  void formatsLogicalAndPhysicalCalciteTrees() {
    ExplainResponseNodeV2 plan = new ExplainResponseNodeV2("logical", "physical", "extended");
    plan.setLogicalTree(Map.of("operator", "LogicalProject"));
    plan.setPhysicalTree(Map.of("operator", "EnumerableCalc"));

    assertEquals(
        "{\"calcite\":{\"logical\":{\"operator\":\"LogicalProject\"},"
            + "\"physical\":{\"operator\":\"EnumerableCalc\"}}}",
        formatter.format(new ExplainResponse(plan)));
  }

  @Test
  void omitsAbsentPhysicalTree() {
    ExplainResponseNodeV2 plan = new ExplainResponseNodeV2("logical", null, null);
    plan.setLogicalTree(Map.of("operator", "LogicalProject"));

    assertEquals(
        "{\"calcite\":{\"logical\":{\"operator\":\"LogicalProject\"}}}",
        formatter.format(new ExplainResponse(plan)));
  }

  @Test
  void preservesTextExplainWhenNoLogicalTreeIsAvailable() {
    ExplainResponseNodeV2 plan = new ExplainResponseNodeV2("logical", "physical", "extended");

    assertEquals(
        "{\"calcite\":{\"logical\":\"logical\",\"physical\":\"physical\","
            + "\"extended\":\"extended\"}}",
        formatter.format(new ExplainResponse(plan)));
  }

  @Test
  void preservesLegacyExplainTree() {
    ExplainResponseNode plan = new ExplainResponseNode("scan", Map.of(), List.of());

    assertEquals(
        "{\"root\":{\"name\":\"scan\",\"description\":{},\"children\":[]}}",
        formatter.format(new ExplainResponse(plan)));
  }
}
