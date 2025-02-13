/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.transport.client.Client;

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
