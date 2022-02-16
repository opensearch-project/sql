/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.client;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.threadpool.ThreadPool;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  /** Default types and field filter to match all. */
  public static final String[] ALL_TYPES = new String[0];

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Current cluster state on local node. */
  private final ClusterService clusterService;

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Index name expression resolver to get concrete index name. */
  private final IndexNameExpressionResolver resolver;

  private static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  /**
   * Constructor of ElasticsearchNodeClient.
   */
  public OpenSearchNodeClient(ClusterService clusterService,
                              NodeClient client) {
    this.clusterService = clusterService;
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
      ClusterState state = clusterService.state();
      String[] concreteIndices = resolveIndexExpression(state, indexExpression);

      return populateIndexMappings(
          state.metadata().findMappings(concreteIndices, ALL_TYPES, ALL_FIELDS));
    } catch (IOException e) {
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
    final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
    builder.put(META_CLUSTER_NAME, clusterService.getClusterName().value());
    return builder.build();
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    request.clean(scrollId -> client.prepareClearScroll().addScrollId(scrollId).get());
  }

  @Override
  public void schedule(Runnable task) {
    ThreadPool threadPool = client.threadPool();
    threadPool.schedule(
        withCurrentContext(task),
        new TimeValue(0),
        SQL_WORKER_THREAD_POOL_NAME
    );
  }

  private String[] resolveIndexExpression(ClusterState state, String[] indices) {
    return resolver.concreteIndexNames(state, IndicesOptions.strictExpandOpen(), true, indices);
  }

  private Map<String, IndexMapping> populateIndexMappings(
      ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetadata>> indexMappings) {

    ImmutableMap.Builder<String, IndexMapping> result = ImmutableMap.builder();
    for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetadata>> cursor :
        indexMappings) {
      result.put(cursor.key, populateIndexMapping(cursor.value));
    }
    return result.build();
  }

  private IndexMapping populateIndexMapping(
      ImmutableOpenMap<String, MappingMetadata> indexMapping) {
    if (indexMapping.isEmpty()) {
      return new IndexMapping(Collections.emptyMap());
    }
    return new IndexMapping(indexMapping.iterator().next().value);
  }

  /** Copy from LogUtils. */
  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
