/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.transport.format.AsyncQueryResultResponseFormatter;
import org.opensearch.sql.spark.transport.model.AsyncQueryResult;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetAsyncQueryResultAction
    extends HandledTransportAction<
        GetAsyncQueryResultActionRequest, GetAsyncQueryResultActionResponse> {

  private final AsyncQueryExecutorService asyncQueryExecutorService;
  private final ClusterService clusterService;
  private final TransportService transportService;

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/result";
  public static final ActionType<GetAsyncQueryResultActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetAsyncQueryResultActionResponse::new);

  @Inject
  public TransportGetAsyncQueryResultAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      AsyncQueryExecutorServiceImpl jobManagementService) {
    super(NAME, transportService, actionFilters, GetAsyncQueryResultActionRequest::new);
    this.asyncQueryExecutorService = jobManagementService;
    this.clusterService = clusterService;
    this.transportService = transportService;
  }

  @Override
  protected void doExecute(
      Task task,
      GetAsyncQueryResultActionRequest request,
      ActionListener<GetAsyncQueryResultActionResponse> listener) {
    try {
      String jobId = request.getQueryId();
      QueryJobId parsedId = tryParseAsJobId(jobId);
      if (parsedId != null) {
        String localNodeId = clusterService.localNode().getId();
        if (!localNodeId.equals(parsedId.ownerNodeId())) {
          forwardToOwner(parsedId, request, listener);
          return;
        }
      }
      AsyncQueryExecutionResponse asyncQueryExecutionResponse =
          asyncQueryExecutorService.getAsyncQueryResults(jobId, new NullAsyncQueryRequestContext());
      ResponseFormatter<AsyncQueryResult> formatter =
          new AsyncQueryResultResponseFormatter(JsonResponseFormatter.Style.PRETTY);
      String responseContent =
          formatter.format(
              new AsyncQueryResult(
                  asyncQueryExecutionResponse.getStatus(),
                  asyncQueryExecutionResponse.getSchema(),
                  asyncQueryExecutionResponse.getResults(),
                  Cursor.None,
                  asyncQueryExecutionResponse.getError()));
      listener.onResponse(new GetAsyncQueryResultActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Parses a queryId as {@link QueryJobId} when the id is opaque and versioned, returning
   * {@code null} for Spark-shaped ids or anything else that does not match the layout. Malformed
   * QueryJobIds also return {@code null} so the caller can fall through to the local Spark path
   * and produce the current "not found" behavior.
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

  /**
   * Forwards the get to the owner node via the same transport action. The owner node's handler
   * resolves the id in its local {@code QueryJobStore} and returns the JSON-formatted response,
   * which is relayed back to the original caller unchanged.
   *
   * @param jobId parsed id whose {@code ownerNodeId} is not this node
   * @param request request received by this entry node
   * @param listener listener returning to the caller
   */
  private void forwardToOwner(
      QueryJobId jobId,
      GetAsyncQueryResultActionRequest request,
      ActionListener<GetAsyncQueryResultActionResponse> listener) {
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
            listener, GetAsyncQueryResultActionResponse::new, org.opensearch.threadpool.ThreadPool.Names.SAME));
  }
}
