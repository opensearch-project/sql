/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

@ExtendWith(MockitoExtension.class)
class VectorSearchIndexTest {

  @Mock private OpenSearchClient client;

  @Mock private Settings settings;

  @Mock private IndexMapping indexMapping;

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
  void buildKnnQueryJsonWithFilterEmbeds() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f, 2.0f},
            Map.of("k", "5"),
            FilterType.EFFICIENT);

    String filterJson = "{\"term\":{\"city\":{\"value\":\"Miami\"}}}";
    String json = index.buildKnnQueryJson(filterJson);

    assertTrue(json.contains("\"filter\""), "Should contain filter field");
    assertTrue(json.contains("\"term\""), "Should contain the filter content");
    assertTrue(json.contains("\"k\":5"), "Should still contain k");
    assertTrue(json.contains("\"vector\":[1.0,2.0]"), "Should contain vector");
  }

  @Test
  void buildKnnQueryJsonWithFilterRadial() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f},
            Map.of("max_distance", "10.5"),
            FilterType.EFFICIENT);

    String filterJson = "{\"range\":{\"rating\":{\"gte\":4.0}}}";
    String json = index.buildKnnQueryJson(filterJson);

    assertTrue(json.contains("\"max_distance\":10.5"), "Should contain max_distance");
    assertTrue(json.contains("\"filter\""), "Should contain filter");
  }

  @Test
  void buildKnnQueryJsonNullFilterProducesBaseJson() {
    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f},
            Map.of("k", "5"),
            null);

    String json = index.buildKnnQueryJson(null);
    String baseJson = index.buildKnnQueryJson();

    assertEquals(baseJson, json, "null filter should produce same JSON as no-arg version");
    assertFalse(json.contains("\"filter\""), "Should not contain filter field");
  }

  @Test
  void buildKnnQueryJsonExcludesFilterType() {
    LinkedHashMap<String, String> options = new LinkedHashMap<>();
    options.put("k", "5");

    VectorSearchIndex index =
        new VectorSearchIndex(
            client,
            settings,
            "test-index",
            "embedding",
            new float[] {1.0f},
            options,
            FilterType.EFFICIENT);

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

  @Test
  void createScanBuilderRejectsIndexWithScoreField() {
    // A mapping that declares a user field named _score cannot coexist with the synthetic
    // v._score column exposed by vectorSearch(); the guard in createScanBuilder should reject
    // it with a clear, user-facing error.
    lenient()
        .when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    when(indexMapping.getFieldMappings())
        .thenReturn(Map.of("_score", OpenSearchDataType.of(MappingType.Float)));
    when(client.getIndexMappings("test-index"))
        .thenReturn(ImmutableMap.of("test-index", indexMapping));

    VectorSearchIndex index =
        new VectorSearchIndex(
            client, settings, "test-index", "embedding", new float[] {1.0f}, Map.of("k", "5"));

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, index::createScanBuilder);
    assertTrue(
        ex.getMessage().contains("_score"),
        "error message should mention the colliding _score field");
    assertTrue(
        ex.getMessage().contains("collides"),
        "error message should describe the collision, got: " + ex.getMessage());
  }
}
