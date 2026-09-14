/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
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
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext.SearchMode;
import org.opensearch.sql.opensearch.executor.ProgressiveQueryContext.SearchOperation;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.transport.client.node.NodeClient;

/** OpenSearch connection by node client. */
public class OpenSearchNodeClient implements OpenSearchClient {
  private static final Logger LOG = LogManager.getLogger(OpenSearchNodeClient.class);
  private static final int PROGRESSIVE_AGGREGATION_BATCHED_REDUCE_SIZE = 5;
  private static final long PROGRESSIVE_AGGREGATION_SNAPSHOT_INTERVAL_NANOS = 500_000_000L;

  public static final Function<String, Predicate<String>> ALL_FIELDS =
      (anyIndex -> (anyField -> true));

  /** Node client provided by OpenSearch container. */
  private final NodeClient client;

  /** Constructor of OpenSearchNodeClient. */
  public OpenSearchNodeClient(NodeClient client) {
    this.client = client;
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
      // Re-throw directly to be treated as client error finally
      throw ErrorReport.wrap(e)
          .code(ErrorCode.INDEX_NOT_FOUND)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
    } catch (OpenSearchSecurityException e) {
      // Re-throw with permission denied code
      throw ErrorReport.wrap(e)
          .code(ErrorCode.PERMISSION_DENIED)
          .location("while fetching index mappings")
          .context("index_name", indexExpression[0])
          .build();
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

  @Override
  public long getIndexDocumentCount(String... indexExpression) {
    try {
      var stats =
          client.admin().indices().prepareStats(indexExpression).clear().setDocs(true).get();
      if (stats.getFailedShards() > 0
          || stats.getPrimaries() == null
          || stats.getPrimaries().getDocs() == null) {
        return -1L;
      }
      return stats.getPrimaries().getDocs().getCount();
    } catch (OpenSearchSecurityException e) {
      LOG.debug(
          "Index document count is unavailable for progress due to permissions: {}",
          Arrays.toString(indexExpression));
      return -1L;
    } catch (Exception e) {
      LOG.debug(
          "Failed to obtain index document count for progress: {}",
          Arrays.toString(indexExpression),
          e);
      return -1L;
    }
  }

  /** TODO: Scroll doesn't work for aggregation. Support aggregation later. */
  @Override
  public OpenSearchResponse search(OpenSearchRequest request) {
    return request.search(
        req -> {
          applyParentTask(req);
          return executeSearch(req, request);
        },
        req -> client.searchScroll(req).actionGet());
  }

  private SearchResponse executeSearch(SearchRequest request, OpenSearchRequest sqlRequest) {
    CancellableTask parentTask = OpenSearchQueryManager.getCancellableTask();
    if (parentTask != null && parentTask.isCancelled()) {
      throw new org.opensearch.core.tasks.TaskCancelledException(parentTask.getReasonCancelled());
    }

    SearchOperation operation = ProgressiveQueryContext.startSearch(searchMode(request));
    if (operation == null) {
      return client.search(request).actionGet();
    }

    int expectedPageUnits = request.source() == null ? 1 : Math.max(1, request.source().size());
    SearchProgressTracker progressTracker =
        new SearchProgressTracker(operation, sqlRequest, expectedPageUnits);
    SearchRequest monitoredRequest =
        new SearchRequest(request) {
          @Override
          public SearchTask createTask(
              long id,
              String type,
              String action,
              TaskId parentTaskId,
              Map<String, String> headers) {
            SearchTask searchTask = super.createTask(id, type, action, parentTaskId, headers);
            searchTask.setProgressListener(progressTracker);
            operation.registerTask(searchTask);
            return searchTask;
          }
        };
    if (sqlRequest.supportsAggregationSnapshots()) {
      monitoredRequest.setBatchedReduceSize(
          Math.min(
              monitoredRequest.getBatchedReduceSize(),
              PROGRESSIVE_AGGREGATION_BATCHED_REDUCE_SIZE));
    }

    try {
      SearchResponse response = client.search(monitoredRequest).actionGet();
      progressTracker.onSearchResponse(response);
      progressTracker.onSearchComplete();
      return response;
    } finally {
      operation.complete();
    }
  }

  private static SearchMode searchMode(SearchRequest request) {
    if (request.source() != null
        && request.source().aggregations() != null
        && request.source().aggregations().getAggregatorFactories().stream()
            .anyMatch(CompositeAggregationBuilder.class::isInstance)) {
      return SearchMode.COMPOSITE;
    }
    if (request.source() != null && request.source().pointInTimeBuilder() != null) {
      return SearchMode.PIT_HITS;
    }
    return SearchMode.SINGLE_REQUEST;
  }

  private void applyParentTask(SearchRequest req) {
    CancellableTask task = OpenSearchQueryManager.getCancellableTask();
    if (task != null) {
      req.setParentTask(new TaskId(client.getLocalNodeId(), task.getId()));
    }
  }

  /** Converts the core search progress callbacks into a PPL job progress snapshot. */
  static final class SearchProgressTracker extends SearchProgressListener {
    private final SearchOperation operation;
    private final OpenSearchRequest request;
    private final int expectedPageUnits;
    private final Set<Integer> completedShardIds = new HashSet<>();
    private final Map<SearchShard, Integer> shardIndexes = new java.util.HashMap<>();
    private int totalShards = -1;
    private int skippedShards;
    private boolean fetchPhase;
    private long lastAggregationSnapshotNanos = Long.MIN_VALUE;

    SearchProgressTracker(
        SearchOperation operation, OpenSearchRequest request, int expectedPageUnits) {
      this.operation = operation;
      this.request = request;
      this.expectedPageUnits = expectedPageUnits;
    }

    @Override
    protected synchronized void onListShards(
        List<SearchShard> shards,
        List<SearchShard> skippedShards,
        SearchResponse.Clusters clusters,
        boolean fetchPhase) {
      this.totalShards = shards.size() + skippedShards.size();
      this.skippedShards = skippedShards.size();
      this.fetchPhase = fetchPhase;
      shardIndexes.clear();
      for (int shardIndex = 0; shardIndex < shards.size(); shardIndex++) {
        shardIndexes.put(shards.get(shardIndex), shardIndex);
      }
      publish();
    }

    @Override
    protected synchronized void onQueryResult(int shardIndex) {
      if (!fetchPhase) {
        completedShardIds.add(shardIndex);
        publish();
      }
    }

    @Override
    protected synchronized void onQueryFailure(
        int shardIndex, SearchShardTarget shardTarget, Exception exc) {
      if (!fetchPhase) {
        completedShardIds.add(shardIndex);
        publish();
      }
    }

    @Override
    protected synchronized void onFetchResult(int shardIndex) {
      completedShardIds.add(shardIndex);
      publish();
    }

    @Override
    protected synchronized void onFetchFailure(
        int shardIndex, SearchShardTarget shardTarget, Exception exc) {
      completedShardIds.add(shardIndex);
      publish();
    }

    @Override
    protected synchronized void onPartialReduce(
        List<SearchShard> shards,
        TotalHits totalHits,
        InternalAggregations aggregations,
        int reducePhase) {
      markReducedShards(shards);
      publish();
      publishAggregationSnapshot(totalHits, aggregations);
    }

    @Override
    protected synchronized void onFinalReduce(
        List<SearchShard> shards,
        TotalHits totalHits,
        InternalAggregations aggregations,
        int reducePhase) {
      markReducedShards(shards);
      publish();
    }

    private synchronized void onSearchComplete() {
      if (!operation.usesPageProgress() && totalShards >= 0) {
        for (int shardIndex = 0; shardIndex < totalShards; shardIndex++) {
          completedShardIds.add(shardIndex);
        }
      }
      publish();
    }

    private void publish() {
      if (operation.usesPageProgress()) {
        if (operation.mode() == SearchMode.PIT_HITS) {
          int completed =
              totalShards < 0 ? 0 : Math.min(totalShards, skippedShards + completedShardIds.size());
          double fraction = totalShards > 0 ? (double) completed / totalShards : 0D;
          operation.publishPageProgress(
              new QueryProgress(Math.min(1D, Math.max(0D, fraction))), expectedPageUnits);
        }
        return;
      }
      int completed =
          totalShards < 0 ? -1 : Math.min(totalShards, skippedShards + completedShardIds.size());
      double fraction = totalShards > 0 ? (double) Math.max(0, completed) / totalShards : 0D;
      operation.publish(new QueryProgress(Math.min(1D, Math.max(0D, fraction))));
    }

    private void markReducedShards(List<SearchShard> shards) {
      for (SearchShard shard : shards) {
        Integer shardIndex = shardIndexes.get(shard);
        if (shardIndex != null) {
          completedShardIds.add(shardIndex);
        }
      }
    }

    private synchronized void onSearchResponse(SearchResponse response) {
      if (!operation.usesPageProgress()) {
        return;
      }
      TotalHits totalHits = response.getHits() == null ? null : response.getHits().getTotalHits();
      long total = totalHits == null ? -1L : totalHits.value();
      boolean exact = totalHits != null && totalHits.relation() == TotalHits.Relation.EQUAL_TO;
      long pageUnits =
          operation.mode() == SearchMode.PIT_HITS
              ? response.getHits().getHits().length
              : compositeDocumentCount(response);
      operation.publishPage(pageUnits, total, exact);
    }

    private static long compositeDocumentCount(SearchResponse response) {
      if (response.getAggregations() == null) {
        return 0L;
      }
      return response.getAggregations().asList().stream()
          .filter(InternalComposite.class::isInstance)
          .map(InternalComposite.class::cast)
          .flatMap(composite -> composite.getBuckets().stream())
          .mapToLong(InternalComposite.InternalBucket::getDocCount)
          .sum();
    }

    private void publishAggregationSnapshot(
        TotalHits totalHits, InternalAggregations aggregations) {
      if (!request.supportsAggregationSnapshots()) {
        return;
      }
      long now = System.nanoTime();
      if (lastAggregationSnapshotNanos != Long.MIN_VALUE
          && now - lastAggregationSnapshotNanos < PROGRESSIVE_AGGREGATION_SNAPSHOT_INTERVAL_NANOS) {
        return;
      }
      List<org.opensearch.sql.data.model.ExprValue> rows =
          request.parseAggregationSnapshot(totalHits, aggregations);
      if (!rows.isEmpty()) {
        lastAggregationSnapshotNanos = now;
        operation.publishAggregationSnapshot(rows);
      }
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
