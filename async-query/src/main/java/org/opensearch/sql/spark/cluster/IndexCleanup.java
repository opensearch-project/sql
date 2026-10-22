/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ScrollableHitSource;
import org.opensearch.sql.opensearch.client.PluginClient;

/** Clean up the old docs for indices. */
public class IndexCleanup {
  private static final Logger LOG = LogManager.getLogger(IndexCleanup.class);

  private final PluginClient client;
  private final ClusterService clusterService;

  public IndexCleanup(PluginClient client, ClusterService clusterService) {
    this.client = client;
    this.clusterService = clusterService;
  }

  /**
   * Delete docs based on query request
   *
   * @param indexName index name
   * @param queryForDeleteByQueryRequest query request
   * @param listener told the number of deleted docs, or the failure if any search or delete in the
   *     request failed
   */
  public void deleteDocsByQuery(
      String indexName, QueryBuilder queryForDeleteByQueryRequest, ActionListener<Long> listener) {
    DeleteByQueryRequest deleteRequest =
        new DeleteByQueryRequest(indexName)
            .setQuery(queryForDeleteByQueryRequest)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setRefresh(true);

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
              Exception failure = failureOf(indexName, response);
              if (failure != null) {
                listener.onFailure(failure);
              } else {
                listener.onResponse(deleted);
              }
            },
            listener::onFailure));
  }

  // Delete-by-query completes normally when searches or deletes in it were rejected, for example
  // when the subject it runs as is not permitted an action, and lists them in the response.
  private static Exception failureOf(String indexName, BulkByScrollResponse response) {
    // A conflict means the doc was written after the search found it, so it is not stale.
    List<BulkItemResponse.Failure> bulkFailures =
        response.getBulkFailures().stream()
            .filter(failure -> failure.getStatus() != RestStatus.CONFLICT)
            .toList();
    List<ScrollableHitSource.SearchFailure> searchFailures = response.getSearchFailures();
    if (bulkFailures.isEmpty() && searchFailures.isEmpty()) {
      return null;
    }
    Throwable cause =
        bulkFailures.isEmpty() ? searchFailures.get(0).getReason() : bulkFailures.get(0).getCause();
    return new IllegalStateException(
        String.format(
            Locale.ROOT,
            "Delete by query on %s had %d delete failures and %d search failures",
            indexName,
            bulkFailures.size(),
            searchFailures.size()),
        cause);
  }
}
