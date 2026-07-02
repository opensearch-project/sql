/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

@ExtendWith(MockitoExtension.class)
class RestEndpointRegistryTest {

  @Mock private OpenSearchClient client;

  @Test
  void resolveAllowListedEndpoint() {
    RestEndpointRegistry.Endpoint endpoint = RestEndpointRegistry.resolve("/_cluster/health");
    assertEquals("/_cluster/health", endpoint.getPath());
    assertEquals(STRING, endpoint.getSchema().get("status"));
    assertEquals(INTEGER, endpoint.getSchema().get("number_of_nodes"));
  }

  @Test
  void resolveRejectsNonAllowListedEndpoint() {
    // A mutating endpoint is simply absent from the registry and is refused here.
    assertThrows(
        IllegalArgumentException.class, () -> RestEndpointRegistry.resolve("/_cluster/reroute"));
    assertThrows(
        IllegalArgumentException.class,
        () -> RestEndpointRegistry.resolve("/services/server/info"));
  }

  @Test
  void validateRejectsUnknownArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("not_allowed", "x"), null, null);
    assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.validate(spec));
  }

  @Test
  void validateAcceptsAllowedArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("local", "true"), null, null);
    RestEndpointRegistry.validate(spec); // no throw
  }

  @Test
  void validateRejectsDroppedLevelArg() {
    // level was dropped (no-op against the fixed cluster-level health schema); now unknown.
    RestSpec spec = new RestSpec("/_cluster/health", Map.of("level", "indices"), null, null);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.validate(spec));
    assertTrue(ex.getMessage().contains("does not accept arg"));
  }

  @Test
  void validateRejectsDroppedFlatSettingsArg() {
    // flat_settings was dropped (redundant: settings are already flattened to dotted keys).
    RestSpec spec = new RestSpec("/_cluster/settings", Map.of("flat_settings", "true"), null, null);
    assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.validate(spec));
  }

  @Test
  void validateAcceptsValidArgValues() {
    RestEndpointRegistry.validate(
        new RestSpec("/_cat/indices", Map.of("health", "green"), null, null));
    RestEndpointRegistry.validate(
        new RestSpec("/_resolve/index", Map.of("expand_wildcards", "open"), null, null));
    RestEndpointRegistry.validate(
        new RestSpec("/_resolve/index", Map.of("expand_wildcards", "open,closed"), null, null));
  }

  @Test
  void validateRejectsBadArgValue() {
    IllegalArgumentException health =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RestEndpointRegistry.validate(
                    new RestSpec("/_cat/indices", Map.of("health", "purple"), null, null)));
    assertTrue(health.getMessage().contains("unsupported value"));

    IllegalArgumentException local =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RestEndpointRegistry.validate(
                    new RestSpec("/_cluster/health", Map.of("local", "maybe"), null, null)));
    assertTrue(local.getMessage().contains("unsupported value"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RestEndpointRegistry.validate(
                new RestSpec(
                    "/_resolve/index", Map.of("expand_wildcards", "sideways"), null, null)));
  }

  @Test
  void resolveRejectsBlankEndpoint() {
    IllegalArgumentException emptyEx =
        assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.resolve(""));
    assertTrue(emptyEx.getMessage().contains("non-empty path"));
    assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.resolve("   "));
    assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.resolve(null));
  }

  @Test
  void validateRejectsNegativeCount() {
    RestSpec spec = new RestSpec("/_cat/indices", Map.of(), -1, null);
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.validate(spec));
    assertTrue(ex.getMessage().contains("non-negative"));
  }

  @Test
  void validateAcceptsZeroCount() {
    RestSpec spec = new RestSpec("/_cat/indices", Map.of(), 0, null);
    RestEndpointRegistry.validate(spec); // no throw: 0 is a valid limit
  }

  @Test
  void validateRejectsTimeoutArg() {
    RestSpec spec = new RestSpec("/_cluster/health", Map.of(), null, "5s");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> RestEndpointRegistry.validate(spec));
    assertTrue(ex.getMessage().contains("timeout"));
  }

  @Test
  void coerceParsesNumericStringValues() {
    // The cat JSON API returns numeric columns as strings; coerce must parse them.
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "green");
    health.put("number_of_nodes", "3");
    when(client.clusterHealth(any())).thenReturn(health);

    RestEndpointRegistry.Endpoint endpoint = RestEndpointRegistry.resolve("/_cluster/health");
    List<ExprValue> rows =
        endpoint.toRows(client, new RestSpec("/_cluster/health", Map.of(), null, null));

    assertEquals(3, rows.get(0).tupleValue().get("number_of_nodes").integerValue());
  }

  @Test
  void coerceThrowsClearErrorOnUncoercibleValue() {
    // A non-numeric value for an INTEGER column must surface a clear client error (HTTP 400),
    // not a raw ClassCastException / NumberFormatException (HTTP 500).
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "green");
    health.put("number_of_nodes", "not-a-number");
    when(client.clusterHealth(any())).thenReturn(health);

    RestEndpointRegistry.Endpoint endpoint = RestEndpointRegistry.resolve("/_cluster/health");
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> endpoint.toRows(client, new RestSpec("/_cluster/health", Map.of(), null, null)));
    assertTrue(ex.getMessage().contains("number_of_nodes"));
    assertTrue(ex.getMessage().contains("not-a-number"));
  }

  @Test
  void clusterHealthRowsAreShapedToFixedSchema() {
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("cluster_name", "test-cluster");
    health.put("status", "green");
    health.put("number_of_nodes", 1);
    when(client.clusterHealth(any())).thenReturn(health);

    RestEndpointRegistry.Endpoint endpoint = RestEndpointRegistry.resolve("/_cluster/health");
    List<ExprValue> rows =
        endpoint.toRows(client, new RestSpec("/_cluster/health", Map.of(), null, null));

    assertEquals(1, rows.size());
    assertEquals("green", rows.get(0).tupleValue().get("status").stringValue());
    assertEquals(1, rows.get(0).tupleValue().get("number_of_nodes").integerValue());
    // a declared column the action did not return becomes null, never absent.
    assertTrue(rows.get(0).tupleValue().get("relocating_shards").isNull());
  }

  @Test
  void catIndicesRowsAreShapedToFixedSchema() {
    Map<String, Object> idx = new LinkedHashMap<>();
    idx.put("index", "books");
    idx.put("health", "yellow");
    idx.put("pri", 1);
    idx.put("rep", 1);
    when(client.catIndices(any())).thenReturn(List.of(idx));

    RestEndpointRegistry.Endpoint endpoint = RestEndpointRegistry.resolve("/_cat/indices");
    List<ExprValue> rows =
        endpoint.toRows(client, new RestSpec("/_cat/indices", Map.of(), null, null));

    assertEquals(1, rows.size());
    assertEquals("books", rows.get(0).tupleValue().get("index").stringValue());
    assertEquals("yellow", rows.get(0).tupleValue().get("health").stringValue());
  }
}
