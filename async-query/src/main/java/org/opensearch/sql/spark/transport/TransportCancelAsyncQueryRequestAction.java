/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import java.util.Optional;
import org.json.JSONObject;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.exceptions.QueryJobForbiddenException;
import org.opensearch.sql.job.exceptions.QueryJobNotFoundException;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
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
      Optional<QueryJobId> parsed = QueryJobId.tryParse(queryId);
      if (parsed.isPresent()
          && !clusterService.localNode().getId().equals(parsed.get().ownerNodeId())) {
        AsyncQueryOwnerRouting.forwardToOwner(
            clusterService,
            transportService,
            parsed.get(),
            NAME,
            request,
            CancelAsyncQueryActionResponse::new,
            ActionListener.wrap(
                listener::onResponse,
                failure ->
                    listener.onFailure(
                        failure instanceof AsyncQueryNotFoundException
                            ? new ResourceNotFoundException(failure.getMessage())
                            : failure)));
        return;
      }
      String result =
          asyncQueryExecutorService.cancelQuery(queryId, new NullAsyncQueryRequestContext());
      listener.onResponse(
          new CancelAsyncQueryActionResponse(
              parsed.isPresent()
                  ? new JSONObject().put("status", result).toString()
                  : String.format("Deleted async query with id: %s", result)));
    } catch (QueryJobNotFoundException e) {
      listener.onFailure(new ResourceNotFoundException(e.getMessage()));
    } catch (QueryJobForbiddenException e) {
      listener.onFailure(new OpenSearchStatusException(e.getMessage(), RestStatus.FORBIDDEN));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
