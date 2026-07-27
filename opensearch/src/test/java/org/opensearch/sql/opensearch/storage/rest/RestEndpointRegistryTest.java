/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.ColumnType;
import org.opensearch.sql.spi.rest.RestEndpointContext;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * Covers the {@code rest} {@link RestEndpointRegistry} against the PR1 endpoint set (only {@code
 * /_cluster/health}): allow-list resolution, arg validation, count/timeout gating, value coercion,
 * and fixed-schema row shaping. Coercion and shaping are exercised through a fake provider that
 * returns canned rows, so they stay independent of how any one endpoint fetches. Redaction-by-class
 * is covered separately in {@link RedactionByClassTest}; here rows are shaped with an empty {@link
 * RedactionRegistry} (no-op).
 */
class RestEndpointRegistryTest {

  private RestEndpointRegistry registry;

  @BeforeEach
  void buildRegistry() {
    registry = new RestEndpointRegistry(List.of(new CoreEndpointsProvider()));
  }

  private static RestEndpointContext ctx(Map<String, String> args) {
    return RestEndpointContext.of(args, null);
  }

  private static RestEndpointRegistry.Endpoint fakeEndpoint(
      List<Column> schema, Map<String, Object> row) {
    RestEndpointProvider provider =
        () ->
            List.of(
                RestEndpointDefinition.builder()
                    .name("/_test/probe")
                    .schema(schema)
                    .handler(context -> List.of(row))
                    .build());
    return new RestEndpointRegistry(List.of(provider)).resolve("/_test/probe");
  }

  @Test
  void resolveAllowListedEndpoint() {
    RestEndpointRegistry.Endpoint endpoint = registry.resolve("/_cluster/health");
    assertEquals("/_cluster/health", endpoint.getPath());
    assertEquals(STRING, endpoint.getSchema().get("status"));
    assertEquals(INTEGER, endpoint.getSchema().get("number_of_nodes"));
  }

  @Test
  void resolveRejectsNonAllowListedEndpoint() {
    // A mutating endpoint, and any endpoint deferred out of PR1, is simply absent and refused here.
    assertThrows(IllegalArgumentException.class, () -> registry.resolve("/_cluster/reroute"));
    assertThrows(IllegalArgumentException.class, () -> registry.resolve("/_cat/nodes"));
    assertThrows(IllegalArgumentException.class, () -> registry.resolve("/services/server/info"));
  }

  @Test
  void resolveRejectsBlankEndpoint() {
    IllegalArgumentException emptyEx =
        assertThrows(IllegalArgumentException.class, () -> registry.resolve(""));
    assertTrue(emptyEx.getMessage().contains("non-empty path"));
    assertThrows(IllegalArgumentException.class, () -> registry.resolve("   "));
    assertThrows(IllegalArgumentException.class, () -> registry.resolve(null));
  }

  @Test
  void validateRejectsUnknownArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("not_allowed", "x"), null, null);
    assertThrows(IllegalArgumentException.class, () -> registry.validate(spec));
  }

  @Test
  void validateAcceptsAllowedArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("local", "true"), null, null);
    registry.validate(spec); // no throw
  }

  @Test
  void validateRejectsDroppedLevelArg() {
    // level was dropped (no-op against the fixed cluster-level health schema); now unknown.
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("level", "indices"), null, null);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registry.validate(spec));
    assertTrue(ex.getMessage().contains("does not accept arg"));
  }

  @Test
  void validateRejectsBadArgValue() {
    IllegalArgumentException local =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                registry.validate(
                    new RestSpec("/_cluster/health", Map.of("local", "maybe"), null, null)));
    assertTrue(local.getMessage().contains("unsupported value"));
  }

  @Test
  void validateRejectsNegativeCount() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of(), -1, null);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registry.validate(spec));
    assertTrue(ex.getMessage().contains("non-negative"));
  }

  @Test
  void validateAcceptsZeroCount() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of(), 0, null);
    registry.validate(spec); // no throw: 0 is a valid limit
  }

  @Test
  void validateRejectsTimeoutArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of(), null, "5s");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> registry.validate(spec));
    assertTrue(ex.getMessage().contains("timeout"));
  }

  @Test
  void coerceParsesNumericStringValues() {
    // The transport/JSON layer may return numeric columns as strings; coerce must parse them.
    RestEndpointRegistry.Endpoint endpoint =
        fakeEndpoint(
            List.of(
                Column.of("status", ColumnType.STRING),
                Column.of("number_of_nodes", ColumnType.INTEGER)),
            Map.<String, Object>of("status", "green", "number_of_nodes", "3"));
    List<ExprValue> rows = endpoint.toRows(ctx(Map.of()), new RedactionRegistry());
    assertEquals(3, rows.get(0).tupleValue().get("number_of_nodes").integerValue());
  }

  @Test
  void coerceThrowsClearErrorOnUncoercibleValue() {
    // A non-numeric value for an INTEGER column must surface a clear client error (HTTP 400),
    // not a raw ClassCastException / NumberFormatException (HTTP 500).
    RestEndpointRegistry.Endpoint endpoint =
        fakeEndpoint(
            List.of(
                Column.of("status", ColumnType.STRING),
                Column.of("number_of_nodes", ColumnType.INTEGER)),
            Map.<String, Object>of("status", "green", "number_of_nodes", "not-a-number"));
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> endpoint.toRows(ctx(Map.of()), new RedactionRegistry()));
    assertTrue(ex.getMessage().contains("number_of_nodes"));
    assertTrue(ex.getMessage().contains("not-a-number"));
  }

  @Test
  void rowsAreShapedToFixedSchema() {
    Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("cluster_name", "test-cluster");
    raw.put("status", "green");
    raw.put("number_of_nodes", 1);
    RestEndpointRegistry.Endpoint endpoint =
        fakeEndpoint(
            List.of(
                Column.of("cluster_name", ColumnType.STRING),
                Column.of("status", ColumnType.STRING),
                Column.of("number_of_nodes", ColumnType.INTEGER),
                Column.of("relocating_shards", ColumnType.INTEGER)),
            raw);
    List<ExprValue> rows = endpoint.toRows(ctx(Map.of()), new RedactionRegistry());
    assertEquals(1, rows.size());
    assertEquals("green", rows.get(0).tupleValue().get("status").stringValue());
    assertEquals(1, rows.get(0).tupleValue().get("number_of_nodes").integerValue());
    // a declared column the handler did not return becomes null, never absent.
    assertTrue(rows.get(0).tupleValue().get("relocating_shards").isNull());
  }
}
