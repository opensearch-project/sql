/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.Redactor;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;

/**
 * Proves the {@code rest} choke point applies the single {@link Redactor} to each raw response row
 * (with the endpoint name as scope) before coercion, and that {@link Redactor#NONE} passes rows
 * through unchanged.
 */
class RedactorSeamTest {

  private static RestEndpointRegistry.Endpoint probe() {
    RestEndpointProvider provider =
        () ->
            List.of(
                RestEndpointDefinition.builder()
                    .name("/_test/probe")
                    .schema(List.of(Column.of("ip"), Column.of("count")))
                    .handler(ctx -> List.of(Map.of("ip", "10.0.0.7", "count", 3)))
                    .build());
    return new RestEndpointRegistry(List.of(provider)).resolve("/_test/probe");
  }

  @Test
  void chokePointAppliesRedactorToRawRowBeforeCoercion() {
    Redactor maskIp =
        (endpoint, row) -> {
          Map<String, Object> masked = new HashMap<>(row);
          masked.put("ip", "x.x.x.x");
          return masked;
        };

    List<ExprValue> rows = probe().toRows(RestEndpointContext.of(Map.of(), null), maskIp);

    assertEquals("x.x.x.x", rows.get(0).tupleValue().get("ip").stringValue());
    assertEquals("3", rows.get(0).tupleValue().get("count").stringValue());
  }

  @Test
  void noneRedactorPassesThrough() {
    List<ExprValue> rows = probe().toRows(RestEndpointContext.of(Map.of(), null), Redactor.NONE);
    assertEquals("10.0.0.7", rows.get(0).tupleValue().get("ip").stringValue());
  }
}
