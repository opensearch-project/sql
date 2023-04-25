/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /**
   * Constructor of OpenSearchNodeClient.
   */
  public OpenSearchNodeClient(NodeClient client) {
    this.client = client;
  }

  @Override
  public boolean exists(String indexName) {
    try {
      IndicesExistsResponse checkExistResponse = client.admin().indices()
          .exists(new IndicesExistsRequest(indexName)).actionGet();
      return checkExistResponse.isExists();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to check if index [" + indexName + "] exists", e);
    }
  }

  @Override
  public void createIndex(String indexName, Map<String, Object> mappings) {
    try {
      // TODO: 1.pass index settings (the number of primary shards, etc); 2.check response?
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).mapping(mappings);
      client.admin().indices().create(createIndexRequest).actionGet();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create index [" + indexName + "]", e);
    }
  }

  /**
   * Get field mappings of index by an index expression. Majority is copied from legacy
   * LocalClusterState.
   *
   * <p>For simplicity, removed type (deprecated) and field filter in argument list. Also removed
   * mapping cache, cluster state listener (mainly for performance and debugging).
   *
   * @param indexExpression index name expression
   * @return index mapping(s) in our class to isolate OpenSearch API. IndexNotFoundException is
   *     thrown if no index matched.
   */
  @Override
  public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
    try {
      GetMappingsResponse mappingsResponse = client.admin().indices()
          .prepareGetMappings(indexExpression)
          .setLocal(true)
          .get();
      return mappingsResponse.mappings().entrySet().stream().collect(Collectors.toUnmodifiableMap(
              Map.Entry::getKey,
              cursor -> new IndexMapping(cursor.getValue())));
    } catch (IndexNotFoundException e) {
      // Re-throw directly to be treated as client error finally
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping for index pattern [" + indexExpression + "]", e);
    }
  }

  /**
   * Fetch index.max_result_window settings according to index expression given.
   *
   * @param indexExpression index expression
   * @return map from index name to its max result window
   */
  @Override
  public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
    try {
      GetSettingsResponse settingsResponse =
          client.admin().indices().prepareGetSettings(indexExpression).setLocal(true).get();
      ImmutableMap.Builder<String, Integer> result = ImmutableMap.builder();
      for (ObjectObjectCursor<String, Settings> indexToSetting :
          settingsResponse.getIndexToSettings()) {
        Settings settings = indexToSetting.value;
        result.put(
            indexToSetting.key,
            settings.getAsInt(
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(settings)));
      }
      return result.build();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read setting for index pattern [" + indexExpression + "]", e);
    }
  }

  /**
   * TODO: Scroll doesn't work for aggregation. Support aggregation later.
   */
  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> client.search(req).actionGet(),
        req -> client.searchScroll(req).actionGet()
    );
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    final GetIndexResponse indexResponse = client.admin().indices()
        .prepareGetIndex()
        .setLocal(true)
        .get();
    final Stream<String> aliasStream =
        ImmutableList.copyOf(indexResponse.aliases().valuesIt()).stream()
            .flatMap(Collection::stream).map(AliasMetadata::alias);

    return Stream.concat(Arrays.stream(indexResponse.getIndices()), aliasStream)
        .collect(Collectors.toList());
  }

  /**
   * Get meta info of the cluster.
   *
   * @return meta info of the cluster.
   */
  @Override
  public Map<String, String> meta() {
    return ImmutableMap.of(META_CLUSTER_NAME,
        client.settings().get("cluster.name", "opensearch"));
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    request.clean(scrollId -> {
      try {
        client.prepareClearScroll().addScrollId(scrollId).get();
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to clean up resources for search request " + request, e);
      }
    });
  }

  @Override
  public void schedule(Runnable task) {
    // at that time, task already running the sql-worker ThreadPool.
    task.run();
  }

  @Override
  public NodeClient getNodeClient() {
    return client;
  }
}
