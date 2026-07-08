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
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.utils.SystemIndexUtils.RestSpec;

/**
 * Covers the {@code rest} {@link RestCatalogSource}: fixed endpoint schema, allow-list enforcement,
 * response row shaping and truncation, the {@code Scannable} opt-in, and the Calcite only (no V2)
 * path.
 */
@ExtendWith(MockitoExtension.class)
class RestCatalogSourceTest {

  @Mock private OpenSearchClient client;

  private RestSpec healthSpec() {
    return new RestSpec("/_cluster/health", Map.of(), null, null);
  }

  @Test
  void getFieldTypesReturnsFixedEndpointSchema() {
    RestCatalogSource source = new RestCatalogSource(client, healthSpec());
    Map<String, ExprType> fieldTypes = source.getFieldTypes();
    assertThat(fieldTypes, hasEntry("status", STRING));
    assertThat(fieldTypes, hasEntry("number_of_nodes", INTEGER));
  }

  @Test
  void isScannable() {
    assertTrue(new RestCatalogSource(client, healthSpec()).isScannable());
  }

  @Test
  void implementV2IsUnsupported() {
    RestCatalogSource source = new RestCatalogSource(client, healthSpec());
    assertThrows(UnsupportedOperationException.class, () -> source.implementV2(null));
  }

  @Test
  void constructorRejectsNonAllowListedEndpoint() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(client, new RestSpec("/_cluster/reroute", Map.of(), null, null)));
  }

  @Test
  void constructorRejectsDisallowedArg() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(
                client, new RestSpec("/_cluster/health", Map.of("bad", "x"), null, null)));
  }

  @Test
  void constructorRejectsNegativeCount() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new RestCatalogSource(client, new RestSpec("/_cat/indices", Map.of(), -1, null)));
  }

  @Test
  void constructorRejectsTimeoutArg() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RestCatalogSource(client, new RestSpec("/_cluster/health", Map.of(), null, "5s")));
  }

  @Test
  void restRequestShapesResponseRows() {
    Map<String, Object> health = new LinkedHashMap<>();
    health.put("status", "green");
    health.put("number_of_nodes", 1);
    when(client.clusterHealth(any())).thenReturn(health);

    RestCatalogSource source = new RestCatalogSource(client, healthSpec());
    List<ExprValue> rows = source.createRequest().search();
    assertEquals(1, rows.size());
    assertEquals("green", rows.get(0).tupleValue().get("status").stringValue());
    assertEquals(1, rows.get(0).tupleValue().get("number_of_nodes").integerValue());
  }

  @Test
  void countTruncatesRows() {
    Map<String, Object> idx1 = new LinkedHashMap<>();
    idx1.put("index", "a");
    Map<String, Object> idx2 = new LinkedHashMap<>();
    idx2.put("index", "b");
    when(client.catIndices(any())).thenReturn(List.of(idx1, idx2));

    RestCatalogSource source =
        new RestCatalogSource(client, new RestSpec("/_cat/indices", Map.of(), 1, null));
    List<ExprValue> rows = source.createRequest().search();
    assertEquals(1, rows.size());
    assertEquals("a", rows.get(0).tupleValue().get("index").stringValue());
  }
}
