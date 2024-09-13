/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.*;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * OpenSearch REST client to support standalone mode that runs entire engine from remote.
 *
 * <p>TODO: Support for authN and authZ with AWS Sigv4 or security plugin.
 */
@RequiredArgsConstructor
public class OpenSearchRestClient implements OpenSearchClient {

  /** OpenSearch high level REST client. */
  private final RestHighLevelClient client;

  @Override
  public boolean exists(String indexName) {
    try {
      return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exist", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      client
          .indices()
          .create(new CreateIndexRequest(indexName).mapping(mappings), RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexExpression);
    try {
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
      return response.mappings().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> new IndexMapping(e.getValue())));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get index mappings for " + indexExpression, e);
    }
  }

  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    GetSettingsRequest request =
        new GetSettingsRequest().indices(indexExpression).includeDefaults(true);
    try {
      GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);
      Map<String, Settings> settings = response.getIndexToSettings();
      Map<String, Settings> defaultSettings = response.getIndexToDefaultSettings();
      Map<String, Integer> result = new HashMap<>();

      defaultSettings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      settings.forEach(
          (key, value) -> {
            Integer maxResultWindow = value.getAsInt("index.max_result_window", null);
            if (maxResultWindow != null) {
              result.put(key, maxResultWindow);
            }
          });

      return result;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get max result window for " + indexExpression, e);
    }
  }

  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> {
          try {
            return client.search(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform search operation with request " + req, e);
          }
        },
        req -> {
          try {
            return client.scroll(req, RequestOptions.DEFAULT);
          } catch (IOException e) {
            throw new IllegalStateException(
                "Failed to perform scroll operation with request " + req, e);
          }
        });
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    try {
      GetIndexResponse indexResponse =
          client.indices().get(new GetIndexRequest(), RequestOptions.DEFAULT);
      final Stream<String> aliasStream =
          ImmutableList.copyOf(indexResponse.getAliases().values()).stream()
              .flatMap(Collection::stream)
              .map(AliasMetadata::alias);
      return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get indices", e);
    }
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    try {
      final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
      ClusterGetSettingsRequest request = new ClusterGetSettingsRequest();
      request.includeDefaults(true);
      request.local(true);
      final Settings defaultSettings =
          client.cluster().getSettings(request, RequestOptions.DEFAULT).getDefaultSettings();
      builder.put(META_CLUSTER_NAME, defaultSettings.get("cluster.name", "opensearch"));
      builder.put(
          "plugins.sql.pagination.api", defaultSettings.get("plugins.sql.pagination.api", "true"));
      return builder.build();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get cluster meta info", e);
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.clean(
          scrollId -> {
            try {
              ClearScrollRequest clearRequest = new ClearScrollRequest();
              clearRequest.addScrollId(scrollId);
              client.clearScroll(clearRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else if (request instanceof OpenSearchQueryRequest) {
      request.clean(pitId -> {});
    }
  }

  @Override
  public void schedule(Runnable task) {
    task.run();
  }

  @Override
  public NodeClient getNodeClient() {
    throw new UnsupportedOperationException("Unsupported method.");
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    try {
      CreatePitResponse createPitResponse =
          client.createPit(createPitRequest, RequestOptions.DEFAULT);
      return createPitResponse.getId();
    } catch (IOException e) {
      throw new RuntimeException("Error occurred while creating PIT for new engine SQL query", e);
    }
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    try {
      DeletePitResponse deletePitResponse =
          client.deletePit(deletePitRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException("Error occurred while creating PIT for new engine SQL query", e);
    }
  }
}
