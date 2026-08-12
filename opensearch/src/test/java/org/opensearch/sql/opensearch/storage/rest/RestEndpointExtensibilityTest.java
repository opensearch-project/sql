/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.ArgSpec;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * Proves the {@code rest} framework treats the built-in {@link CoreEndpointsProvider} and an
 * externally contributed {@link RestEndpointProvider} as uniform clients of one registry: endpoints
 * from BOTH resolve, validate against the same allow-list, and fetch rows the same way. The
 * built-in provider holds no privileged position.
 */
class RestEndpointExtensibilityTest {

  /** A stand-in external plugin provider: contributes one endpoint that echoes a query arg. */
  private static final class FakeEchoProvider implements RestEndpointProvider {
    @Override
    public List<RestEndpointDefinition> getEndpoints() {
      return List.of(
          RestEndpointDefinition.builder()
              .name("/_plugin/echo")
              .argSpec(ArgSpec.builder().arg("text").build())
              .handler(ctx -> List.of(ctx.args().getOrDefault("text", "default")))
              .build());
    }
  }

  private RestEndpointRegistry mergedRegistry() {
    return new RestEndpointRegistry(List.of(new CoreEndpointsProvider(), new FakeEchoProvider()));
  }

  @Test
  void bothBuiltInAndExternalEndpointsResolve() {
    RestEndpointRegistry registry = mergedRegistry();

    assertEquals("/_cluster/health", registry.resolve("/_cluster/health").getPath());
    assertEquals("/_plugin/echo", registry.resolve("/_plugin/echo").getPath());
  }

  @Test
  void externalEndpointFetchesThroughTheSamePath() {
    RestEndpointRegistry.Endpoint echo = mergedRegistry().resolve("/_plugin/echo");
    List<ExprValue> rows = echo.toRows(RestEndpointContext.of(Map.of("text", "hi"), null));
    assertEquals(1, rows.size());
    assertEquals("hi", rows.get(0).tupleValue().get("response").stringValue());
  }

  @Test
  void externalEndpointArgsValidatedByTheSameAllowList() {
    RestEndpointRegistry registry = mergedRegistry();
    // Declared arg is accepted.
    registry.validate(new RestSpec("/_plugin/echo", Map.of("text", "x"), null, null));
    // Undeclared arg is rejected by the same validation path that guards built-in endpoints.
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                registry.validate(
                    new RestSpec("/_plugin/echo", Map.of("not_allowed", "x"), null, null)));
    assertTrue(ex.getMessage().contains("does not accept arg"));
  }

  @Test
  void duplicateOfBuiltInNameIsDroppedSoBuiltInWins() {
    // An external provider that re-declares a built-in name (/_cluster/health) is dropped rather
    // than failing the build; a built-in name cannot be shadowed, so the built-in wins.
    RestEndpointProvider shadowsCore =
        () ->
            List.of(
                RestEndpointDefinition.builder()
                    .name("/_cluster/health")
                    .handler(ctx -> List.of())
                    .build());
    RestEndpointRegistry registry =
        new RestEndpointRegistry(List.of(new CoreEndpointsProvider(), shadowsCore));

    assertEquals("/_cluster/health", registry.resolve("/_cluster/health").getPath());
    assertTrue(registry.resolve("/_cluster/health").isBuiltIn());
  }
}
