/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Index name expression resolver to get concrete index name. */
  private final IndexNameExpressionResolver resolver;

  /**
   * Constructor of ElasticsearchNodeClient.
   */
  public OpenSearchNodeClient(NodeClient client) {
    this.client = client;
    this.resolver = new IndexNameExpressionResolver(client.threadPool().getThreadContext());
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
      return Streams.stream(mappingsResponse.mappings().iterator())
          .collect(Collectors.toMap(cursor -> cursor.key,
              cursor -> new IndexMapping(cursor.value)));
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping in cluster state for index pattern [" + indexExpression + "]", e);
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
    request.clean(scrollId -> client.prepareClearScroll().addScrollId(scrollId).get());
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
