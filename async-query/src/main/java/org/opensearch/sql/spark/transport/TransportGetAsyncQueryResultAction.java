/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport;

import java.util.Optional;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.protocol.response.format.ExplainResponseJsonFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
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
      Optional<QueryJobId> parsed = QueryJobId.tryParse(jobId);
      if (parsed.isPresent()
          && !clusterService.localNode().getId().equals(parsed.get().ownerNodeId())) {
        AsyncQueryOwnerRouting.forwardToOwner(
            clusterService,
            transportService,
            parsed.get(),
            NAME,
            request,
            GetAsyncQueryResultActionResponse::new,
            listener);
        return;
      }
      AsyncQueryExecutionResponse asyncQueryExecutionResponse =
          asyncQueryExecutorService.getAsyncQueryResults(jobId, new NullAsyncQueryRequestContext());
      // Statement-level explain results use the sync explain formatter so the response shape
      // matches the sync explain path byte-for-byte.
      if (asyncQueryExecutionResponse.getExplain() != null) {
        listener.onResponse(
            new GetAsyncQueryResultActionResponse(
                new ExplainResponseJsonFormatter(JsonResponseFormatter.Style.PRETTY)
                    .format(asyncQueryExecutionResponse.getExplain())));
        return;
      }
      ResponseFormatter<AsyncQueryResult> formatter =
          new AsyncQueryResultResponseFormatter(JsonResponseFormatter.Style.PRETTY);
      String responseContent =
          formatter.format(
              new AsyncQueryResult(
                  asyncQueryExecutionResponse.getStatus(),
                  asyncQueryExecutionResponse.getSchema(),
                  asyncQueryExecutionResponse.getResults(),
                  Cursor.None,
                  asyncQueryExecutionResponse.getError(),
                  asyncQueryExecutionResponse.getErrorDetails()));
      listener.onResponse(new GetAsyncQueryResultActionResponse(responseContent));
    } catch (org.opensearch.sql.job.exceptions.QueryJobNotFoundException e) {
      // Translate to a transport-serializable OpenSearchException so cross-node forwarding
      // preserves the 404 status (otherwise the forwarded exception arrives on the entry node
      // as NotSerializableExceptionWrapper mapped to 500).
      listener.onFailure(new org.opensearch.ResourceNotFoundException(e.getMessage()));
    } catch (org.opensearch.sql.job.exceptions.QueryJobForbiddenException e) {
      listener.onFailure(
          new org.opensearch.OpenSearchStatusException(
              e.getMessage(), org.opensearch.core.rest.RestStatus.FORBIDDEN));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
