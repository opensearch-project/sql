/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.directquery.DirectQueryExecutorService;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionRequest;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportExecuteDirectQueryRequestAction
    extends HandledTransportAction<
        ExecuteDirectQueryActionRequest, ExecuteDirectQueryActionResponse> {

  private final DirectQueryExecutorService directQueryExecutorService;

  public static final String NAME = "indices:data/read/direct_query";
  public static final ActionType<ExecuteDirectQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, ExecuteDirectQueryActionResponse::new);

  @Inject
  public TransportExecuteDirectQueryRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DirectQueryExecutorServiceImpl directQueryExecutorService) {
    super(NAME, transportService, actionFilters, ExecuteDirectQueryActionRequest::new);
    this.directQueryExecutorService = directQueryExecutorService;
  }

  @Override
  protected void doExecute(
      Task task,
      ExecuteDirectQueryActionRequest request,
      ActionListener<ExecuteDirectQueryActionResponse> listener) {
    try {
      ExecuteDirectQueryRequest directQueryRequest = request.getDirectQueryRequest();

      ExecuteDirectQueryResponse response =
          directQueryExecutorService.executeDirectQuery(directQueryRequest);

      // Pass the data source name from the request to the response constructor
      listener.onResponse(
          new ExecuteDirectQueryActionResponse(
              response.getQueryId(),
              response.getResult(),
              response.getSessionId(),
              directQueryRequest.getDataSources(),
              response.getDataSourceType()));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
