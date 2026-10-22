/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.search.*;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.response.ShardStats;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /**
   * Thread-context key where the security plugin stores the request's user (the value of
   * ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT in opensearch-commons).
   */
  private static final String SECURITY_USER_INFO = "_opendistro_security_user_info";

  /** Longest wait for a remote cluster's mappings or settings. */
  private static final TimeValue REMOTE_METADATA_TIMEOUT = TimeValue.timeValueSeconds(10);

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Resolves "cluster:index" names; only asked for when a name has a cluster prefix. */
  private final Supplier<RemoteClusterService> remoteClusters;

  /** Constructor of OpenSearchNodeClient. */
  public OpenSearchNodeClient(NodeClient client, Supplier<RemoteClusterService> remoteClusters) {
    this.client = client;
    this.remoteClusters = remoteClusters;
  }

  @Override
  public boolean exists(String indexName) {
    try {
      IndicesExistsResponse checkExistResponse =
          client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
      return checkExistResponse.isExists();
    } catch (OpenSearchSecurityException e) {
      throw e;
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
    IndexGroups groups = groupByCluster(indexExpression);
    if (groups.remote().isEmpty()) {
      return localIndexMappings(indexExpression);
    }
    Map<String, IndexMapping> mappings = new HashMap<>();
    if (groups.local().length > 0) {
      mappings.putAll(localIndexMappings(groups.local()));
    }
    mappings.putAll(readRemote(groups).mappings());
    return mappings;
  }

  private Map<String, IndexMapping> localIndexMappings(String... indexExpression) {
    try {
      GetMappingsResponse mappingsResponse =
          client.admin().indices().prepareGetMappings(indexExpression).setLocal(true).get();
      if (mappingsResponse.mappings().isEmpty()) {
        throw new IndexNotFoundException(indexExpression[0]);
      }
      return mappingsResponse.mappings().entrySet().stream()
          .collect(
              Collectors.toUnmodifiableMap(
                  Map.Entry::getKey, cursor -> new IndexMapping(cursor.getValue())));
    } catch (IndexNotFoundException e) {
      throw IndexMappingErrors.indexNotFound(e, indexExpression[0]);
    } catch (OpenSearchSecurityException e) {
      throw IndexMappingErrors.permissionDenied(e, indexExpression[0]);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read mapping for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
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
    IndexGroups groups = groupByCluster(indexExpression);
    if (groups.remote().isEmpty()) {
      return localIndexMaxResultWindows(indexExpression);
    }
    Map<String, Integer> windows = new HashMap<>();
    if (groups.local().length > 0) {
      windows.putAll(localIndexMaxResultWindows(groups.local()));
    }
    // Read with the mappings and cached together, so this is a cache hit after getIndexMappings.
    windows.putAll(readRemote(groups).windows());
    return windows;
  }

  /**
   * The remote indices' mappings and settings, read from each remote cluster and cached. If every
   * remote cluster was skipped but local indices remain, the remotes add nothing and the local
   * indices answer alone, as in a search. With no local indices there are no fields to query, so it
   * is an error.
   */
  private RemoteMappingsReader.RemoteMetadata readRemote(IndexGroups groups) {
    try {
      return new RemoteMappingsReader(client, remoteClusters.get(), REMOTE_METADATA_TIMEOUT)
          .read(
              client.threadPool().getThreadContext().getTransient(SECURITY_USER_INFO),
              groups.remote());
    } catch (RemoteMappingsReader.AllClustersSkipped e) {
      if (groups.local().length == 0) {
        throw IndexMappingErrors.allClustersSkipped(e.getMessage(), e.indexNames);
      }
      return new RemoteMappingsReader.RemoteMetadata(Map.of(), Map.of());
    }
  }

  /** Local index names, and remote cluster alias to the index names asked of it. */
  private record IndexGroups(String[] local, Map<String, String[]> remote) {}

  /**
   * Groups an expression by cluster. A name is remote only if its prefix is a registered remote
   * cluster (or a pattern matching one), as for search, so a local date-math name containing ':'
   * stays local.
   */
  private IndexGroups groupByCluster(String... indexExpression) {
    if (Arrays.stream(indexExpression)
        .noneMatch(name -> name.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0)) {
      return new IndexGroups(indexExpression, Map.of());
    }
    Map<String, String[]> byCluster = new LinkedHashMap<>();
    remoteClusters
        .get()
        .groupIndices(DEFAULT_INDICES_OPTIONS, indexExpression, index -> false)
        .forEach((clusterAlias, indices) -> byCluster.put(clusterAlias, indices.indices()));
    String[] local = byCluster.remove(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
    return new IndexGroups(local == null ? new String[0] : local, byCluster);
  }

  private Map<String, Integer> localIndexMaxResultWindows(String... indexExpression) {
    try {
      GetSettingsResponse settingsResponse =
          client.admin().indices().prepareGetSettings(indexExpression).setLocal(true).get();
      ImmutableMap.Builder<String, Integer> result = ImmutableMap.builder();
      for (Map.Entry<String, Settings> indexToSetting :
          settingsResponse.getIndexToSettings().entrySet()) {
        Settings settings = indexToSetting.getValue();
        result.put(
            indexToSetting.getKey(),
            settings.getAsInt(
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey(),
                IndexSettings.MAX_RESULT_WINDOW_SETTING.getDefault(settings)));
      }
      return result.build();
    } catch (OpenSearchSecurityException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to read setting for index pattern ["
              + String.join(",", indexExpression)
              + "]: "
              + e.getMessage(),
          e);
    }
  }

  /** TODO: Scroll doesn't work for aggregation. Support aggregation later. */
  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> {
          applyParentTask(req);
          return client.search(req).actionGet();
        },
        req -> client.searchScroll(req).actionGet());
  }

  private void applyParentTask(SearchRequest req) {
    CancellableTask task = OpenSearchQueryManager.getCancellableTask();
    if (task != null) {
      req.setParentTask(new TaskId(client.getLocalNodeId(), task.getId()));
    }
  }

  /**
   * Get the combination of the indices and the alias.
   *
   * @return the combination of the indices and the alias
   */
  @Override
  public List<String> indices() {
    final GetIndexResponse indexResponse =
        client.admin().indices().prepareGetIndex().setLocal(true).get();
    final Stream<String> aliasStream =
        ImmutableList.copyOf(indexResponse.aliases().values()).stream()
            .flatMap(Collection::stream)
            .map(AliasMetadata::alias);

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
    return ImmutableMap.of(
        META_CLUSTER_NAME,
        client.settings().get("cluster.name", "opensearch"),
        "plugins.sql.pagination.api",
        client.settings().get("plugins.sql.pagination.api", "true"));
  }

  @Override
  public void forceCleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.forceClean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.forceClean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void cleanup(OpenSearchRequest request) {
    if (request instanceof OpenSearchScrollRequest) {
      request.clean(
          scrollId -> {
            try {
              client.prepareClearScroll().addScrollId(scrollId).get();
            } catch (Exception e) {
              throw new IllegalStateException(
                  "Failed to clean up resources for search request " + request, e);
            }
          });
    } else {
      request.clean(
          pitId -> {
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitId);
            deletePit(deletePitRequest);
          });
    }
  }

  @Override
  public void schedule(Runnable task) {
    // at that time, task already running the sql-worker ThreadPool.
    task.run();
  }

  @Override
  public Optional<NodeClient> getNodeClient() {
    return Optional.of(client);
  }

  @Override
  public String createPit(CreatePitRequest createPitRequest) {
    ActionFuture<CreatePitResponse> execute =
        this.client.execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      ShardStats.from(pitResponse).toWarning().ifPresent(CalcitePlanContext::addWarning);
      return pitResponse.getId();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while creating PIT for internal plugin operation", e);
    }
  }

  @Override
  public void deletePit(DeletePitRequest deletePitRequest) {
    ActionFuture<DeletePitResponse> execute =
        this.client.execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      execute.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof OpenSearchSecurityException) {
        throw (OpenSearchSecurityException) e.getCause();
      }
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Error occurred while deleting PIT for internal plugin operation", e);
    }
  }
}
