/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import java.util.Arrays;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.store.StoreStats;

/** Clean up the old docs for indices. */
public class IndexCleanup {
  private static final Logger LOG = LogManager.getLogger(IndexCleanup.class);

  private final Client client;
  private final ClusterService clusterService;

  public IndexCleanup(Client client, ClusterService clusterService) {
    this.client = client;
    this.clusterService = clusterService;
  }

  /**
   * delete docs when shard size is bigger than max limitation.
   *
   * @param indexName index name
   * @param maxShardSize max shard size
   * @param queryForDeleteByQueryRequest query request
   * @param listener action listener
   */
  public void deleteDocsBasedOnShardSize(
      String indexName,
      long maxShardSize,
      QueryBuilder queryForDeleteByQueryRequest,
      ActionListener<Boolean> listener) {

    if (!clusterService.state().getRoutingTable().hasIndex(indexName)) {
      LOG.debug("skip as the index:{} doesn't exist", indexName);
      return;
    }

    ActionListener<IndicesStatsResponse> indicesStatsResponseListener =
        ActionListener.wrap(
            indicesStatsResponse -> {
              // Check if any shard size is bigger than maxShardSize
              boolean cleanupNeeded =
                  Arrays.stream(indicesStatsResponse.getShards())
                      .map(ShardStats::getStats)
                      .filter(Objects::nonNull)
                      .map(CommonStats::getStore)
                      .filter(Objects::nonNull)
                      .map(StoreStats::getSizeInBytes)
                      .anyMatch(size -> size > maxShardSize);

              if (cleanupNeeded) {
                deleteDocsByQuery(
                    indexName,
                    queryForDeleteByQueryRequest,
                    ActionListener.wrap(r -> listener.onResponse(true), listener::onFailure));
              } else {
                listener.onResponse(false);
              }
            },
            listener::onFailure);

    getCheckpointShardStoreStats(indexName, indicesStatsResponseListener);
  }

  private void getCheckpointShardStoreStats(
      String indexName, ActionListener<IndicesStatsResponse> listener) {
    IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
    indicesStatsRequest.store();
    indicesStatsRequest.indices(indexName);
    client.admin().indices().stats(indicesStatsRequest, listener);
  }

  /**
   * Delete docs based on query request
   *
   * @param indexName index name
   * @param queryForDeleteByQueryRequest query request
   * @param listener action listener
   */
  public void deleteDocsByQuery(
      String indexName, QueryBuilder queryForDeleteByQueryRequest, ActionListener<Long> listener) {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(indexName)
            .setQuery(queryForDeleteByQueryRequest)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setRefresh(true);

    try (ThreadContext.StoredContext context =
        client.threadPool().getThreadContext().stashContext()) {
      client.execute(
          DeleteByQueryAction.INSTANCE,
          deleteRequest,
          ActionListener.wrap(
              response -> {
                long deleted = response.getDeleted();
                if (deleted > 0) {
                  // if 0 docs get deleted, it means our query cannot find any matching doc
                  // or the index does not exist at all
                  LOG.info("{} docs are deleted for index:{}", deleted, indexName);
                }
                listener.onResponse(response.getDeleted());
              },
              listener::onFailure));
    }
  }
}
