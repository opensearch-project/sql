/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCancelAsyncQueryRequestAction
    extends HandledTransportAction<CancelAsyncQueryActionRequest, CancelAsyncQueryActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/delete";
  private final AsyncQueryExecutorServiceImpl asyncQueryExecutorService;
  private final ClusterService clusterService;
  private final TransportService transportService;
  public static final ActionType<CancelAsyncQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CancelAsyncQueryActionResponse::new);

  @Inject
  public TransportCancelAsyncQueryRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      AsyncQueryExecutorServiceImpl asyncQueryExecutorService) {
    super(NAME, transportService, actionFilters, CancelAsyncQueryActionRequest::new);
    this.asyncQueryExecutorService = asyncQueryExecutorService;
    this.clusterService = clusterService;
    this.transportService = transportService;
  }

  @Override
  protected void doExecute(
      Task task,
      CancelAsyncQueryActionRequest request,
      ActionListener<CancelAsyncQueryActionResponse> listener) {
    try {
      String queryId = request.getQueryId();
      QueryJobId parsedId = tryParseAsJobId(queryId);
      if (parsedId != null) {
        String localNodeId = clusterService.localNode().getId();
        if (!localNodeId.equals(parsedId.ownerNodeId())) {
          forwardToOwner(parsedId, request, listener);
          return;
        }
      }
      String cancelledId =
          asyncQueryExecutorService.cancelQuery(queryId, new NullAsyncQueryRequestContext());
      listener.onResponse(
          new CancelAsyncQueryActionResponse(
              String.format("Deleted async query with id: %s", cancelledId)));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Parses a queryId as {@link QueryJobId} when the id is opaque and versioned. Returns {@code
   * null} for Spark-shaped ids and for malformed opaque ids; the caller then falls through to the
   * local Spark path.
   */
  private static QueryJobId tryParseAsJobId(String queryId) {
    if (queryId == null || queryId.isBlank()) {
      return null;
    }
    try {
      return QueryJobId.parse(queryId);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** Forwards the cancel to the owner node via the same transport action. */
  private void forwardToOwner(
      QueryJobId jobId,
      CancelAsyncQueryActionRequest request,
      ActionListener<CancelAsyncQueryActionResponse> listener) {
    DiscoveryNode ownerNode = clusterService.state().nodes().get(jobId.ownerNodeId());
    if (ownerNode == null) {
      listener.onFailure(
          new org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException(
              "QueryId: " + jobId.encode() + " not found"));
      return;
    }
    transportService.sendRequest(
        ownerNode,
        NAME,
        request,
        TransportRequestOptions.EMPTY,
        new ActionListenerResponseHandler<>(
            listener,
            CancelAsyncQueryActionResponse::new,
            org.opensearch.threadpool.ThreadPool.Names.SAME));
  }
}
