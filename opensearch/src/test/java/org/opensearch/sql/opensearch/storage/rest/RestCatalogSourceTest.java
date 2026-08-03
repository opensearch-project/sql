/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.spi.rest.Column;
import org.opensearch.sql.spi.rest.RestEndpointDefinition;
import org.opensearch.sql.spi.rest.RestEndpointProvider;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * Covers the {@code rest} {@link RestCatalogSource} against the PR1 endpoint set (only {@code
 * /_cluster/health}): fixed endpoint schema, allow-list enforcement, response row shaping and
 * truncation, the {@code Scannable} opt-in, and the Calcite only (no V2) path. Schema and gating
 * resolve against a registry built from the built-in {@link CoreEndpointsProvider}; row shaping and
 * truncation use a fake provider returning canned rows, independent of the health transport fetch.
 */
@ExtendWith(MockitoExtension.class)
class RestCatalogSourceTest {

  @Mock private OpenSearchClient client;

  private RestEndpointRegistry registry;

  @BeforeEach
  void buildRegistry() {
    registry = new RestEndpointRegistry(List.of(new CoreEndpointsProvider()));
  }

  private RestSpec healthSpec() {
    return new RestSpec("/_cluster/health", Map.of(), null, null);
  }

  private static RestEndpointRegistry fakeHealthRegistry(List<Map<String, Object>> rows) {
    RestEndpointProvider provider =
        () ->
            List.of(
                RestEndpointDefinition.builder()
                    .name("/_cluster/health")
                    .schema(List.of(Column.of("status"), Column.of("number_of_nodes")))
                    .handler(ctx -> rows)
                    .build());
    return new RestEndpointRegistry(List.of(provider));
  }

  @Test
  void getFieldTypesReturnsFixedEndpointSchema() {
    RestCatalogSource source = new RestCatalogSource(registry, healthSpec(), client);
    Map<String, ExprType> fieldTypes = source.getFieldTypes();
    assertThat(fieldTypes, hasEntry("response", STRING));
  }

  @Test
  void isScannable() {
    assertTrue(new RestCatalogSource(registry, healthSpec(), client).isScannable());
  }

  @Test
  void implementV2IsUnsupported() {
    RestCatalogSource source = new RestCatalogSource(registry, healthSpec(), client);
    assertThrows(UnsupportedOperationException.class, () -> source.implementV2(null));
  }

  @Test
  void constructorRejectsNonAllowListedEndpoint() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(
                registry, new RestSpec("/_cluster/reroute", Map.of(), null, null), client));
  }

  @Test
  void constructorRejectsDisallowedArg() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(
                registry,
                new RestSpec("/_cluster/health", Map.of("bad", "x"), null, null),
                client));
  }

  @Test
  void constructorRejectsNegativeCount() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(
                registry, new RestSpec("/_cluster/health", Map.of(), -1, null), client));
  }

  @Test
  void constructorRejectsTimeoutArg() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(
                registry, new RestSpec("/_cluster/health", Map.of(), null, "5s"), client));
  }

  @Test
  void restRequestShapesResponseRows() {
    when(client.getNodeClient()).thenReturn(Optional.empty());
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "green");
    health.put("number_of_nodes", 1);
    RestCatalogSource source =
        new RestCatalogSource(fakeHealthRegistry(List.of(health)), healthSpec(), client);
    List<ExprValue> rows = source.createRequest().search();
    assertEquals(1, rows.size());
    assertEquals("green", rows.get(0).tupleValue().get("status").stringValue());
    assertEquals("1", rows.get(0).tupleValue().get("number_of_nodes").stringValue());
  }

  @Test
  void countTruncatesRows() {
    // count=0 exercises the truncation path (subList to empty) over a single-row response.
    when(client.getNodeClient()).thenReturn(Optional.empty());
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "green");
    RestCatalogSource source =
        new RestCatalogSource(
            fakeHealthRegistry(List.of(health)),
            new RestSpec("/_cluster/health", Map.of(), 0, null),
            client);
    assertTrue(source.createRequest().search().isEmpty());
  }
}
