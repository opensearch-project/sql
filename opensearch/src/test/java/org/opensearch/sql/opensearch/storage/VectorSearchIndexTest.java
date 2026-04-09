/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class VectorSearchIndexTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Test
  void buildKnnQueryJsonTopK() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f, 2.0f, 3.0f},
            Map.of("k", "5"));

    String json = index.buildKnnQueryJson();
    assertEquals("{\"knn\":{\"embedding\":{\"vector\":[1.0,2.0,3.0],\"k\":5}}}", json);
  }

  @Test
  void buildKnnQueryJsonRadialMaxDistance() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f, 2.0f},
            Map.of("max_distance", "10.5"));

    String json = index.buildKnnQueryJson();
    assertEquals("{\"knn\":{\"embedding\":{\"vector\":[1.0,2.0],\"max_distance\":10.5}}}", json);
  }

  @Test
  void buildKnnQueryJsonRadialMinScore() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {0.5f},
            Map.of("min_score", "0.8"));

    String json = index.buildKnnQueryJson();
    assertEquals("{\"knn\":{\"embedding\":{\"vector\":[0.5],\"min_score\":0.8}}}", json);
  }

  @Test
  void buildKnnQueryJsonNestedFieldName() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "doc.embedding",
            new float[] {1.0f, 2.0f},
            Map.of("k", "10"));

    String json = index.buildKnnQueryJson();
    assertTrue(json.contains("\"doc.embedding\""), "Should contain nested field name with dot");
  }

  @Test
  void buildKnnQueryJsonMultiElementVector() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f, -2.5f, 0.0f, 3.14f, 100.0f},
            Map.of("k", "3"));

    String json = index.buildKnnQueryJson();
    assertTrue(
        json.contains("[1.0,-2.5,0.0,3.14,100.0]"),
        "Should contain all vector components with correct comma separation");
  }

  @Test
  void buildKnnQueryJsonSingleElementVector() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding", new float[] {42.0f}, Map.of("k", "1"));

    String json = index.buildKnnQueryJson();
    assertTrue(json.contains("[42.0]"), "Should contain single-element vector");
  }

  @Test
  void buildKnnQueryJsonNumericOptionRenderedUnquoted() {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    options.put("k", "5");

    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding", new float[] {1.0f}, options);

    String json = index.buildKnnQueryJson();
    assertTrue(json.contains("\"k\":5"), "Numeric option should be unquoted");
  }

  @Test
  void buildKnnQueryJsonNonNumericOptionRenderedQuoted() {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    options.put("k", "5");
    options.put("method", "hnsw");

    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding", new float[] {1.0f}, options);

    String json = index.buildKnnQueryJson();
    assertTrue(json.contains("\"method\":\"hnsw\""), "Non-numeric option should be quoted");
    assertTrue(json.contains("\"k\":5"), "Numeric option should be unquoted");
  }

  @Test
  void buildKnnQueryJsonExcludesFilterType() {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    options.put("k", "5");

    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding",
            new float[] {1.0f}, options, FilterType.EFFICIENT);

    String json = index.buildKnnQueryJson();
    assertFalse(json.contains("filter_type"), "filter_type should not appear in knn JSON");
    assertTrue(json.contains("\"k\":5"), "k should still be present");
  }

  @Test
  void isInstanceOfOpenSearchIndex() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding", new float[] {1.0f}, Map.of("k", "5"));
    assertTrue(index instanceof OpenSearchIndex);
  }
}
