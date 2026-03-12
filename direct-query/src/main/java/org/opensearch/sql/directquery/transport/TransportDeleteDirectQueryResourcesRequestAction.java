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
import org.opensearch.sql.directquery.rest.model.DeleteDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.DeleteDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.DeleteDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.DeleteDirectQueryResourcesActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/*
 * @opensearch.experimental
 */
public class TransportDeleteDirectQueryResourcesRequestAction
    extends HandledTransportAction<
        DeleteDirectQueryResourcesActionRequest, DeleteDirectQueryResourcesActionResponse> {

  private final DirectQueryExecutorService directQueryExecutorService;

  public static final String NAME = "cluster:admin/opensearch/direct_query/delete/resources";
  public static final ActionType<DeleteDirectQueryResourcesActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, DeleteDirectQueryResourcesActionResponse::new);

  @Inject
  public TransportDeleteDirectQueryResourcesRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DirectQueryExecutorServiceImpl directQueryExecutorService) {
    super(NAME, transportService, actionFilters, DeleteDirectQueryResourcesActionRequest::new);
    this.directQueryExecutorService = (DirectQueryExecutorService) directQueryExecutorService;
  }

  @Override
  protected void doExecute(
      Task task,
      DeleteDirectQueryResourcesActionRequest request,
      ActionListener<DeleteDirectQueryResourcesActionResponse> listener) {
    try {
      DeleteDirectQueryResourcesRequest directQueryRequest = request.getDirectQueryRequest();

      DeleteDirectQueryResourcesResponse response =
          directQueryExecutorService.deleteDirectQueryResources(directQueryRequest);
      String responseContent =
          new JsonResponseFormatter<DeleteDirectQueryResourcesResponse>(
              JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(DeleteDirectQueryResourcesResponse response) {
              return response;
            }
          }.format(response);
      listener.onResponse(new DeleteDirectQueryResourcesActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
