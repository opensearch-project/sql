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
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.ReadDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.ReadDirectQueryResourcesActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/*
 * @opensearch.experimental
 */
public class TransportGetDirectQueryResourcesRequestAction
    extends HandledTransportAction<
    ReadDirectQueryResourcesActionRequest, ReadDirectQueryResourcesActionResponse> {

  private final DirectQueryExecutorService directQueryExecutorService;

  public static final String NAME = "cluster:admin/opensearch/direct_query/read/resources";
  public static final ActionType<ReadDirectQueryResourcesActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, ReadDirectQueryResourcesActionResponse::new);

  @Inject
  public TransportGetDirectQueryResourcesRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DirectQueryExecutorServiceImpl directQueryExecutorService) {
    super(NAME, transportService, actionFilters, ReadDirectQueryResourcesActionRequest::new);
    this.directQueryExecutorService = (DirectQueryExecutorService) directQueryExecutorService;
  }

  @Override
  protected void doExecute(
      Task task,
      ReadDirectQueryResourcesActionRequest request,
      ActionListener<ReadDirectQueryResourcesActionResponse> listener) {
    try {
      GetDirectQueryResourcesRequest directQueryRequest = request.getDirectQueryRequest();

      GetDirectQueryResourcesResponse response =
          directQueryExecutorService.getDirectQueryResources(directQueryRequest);
      String responseContent =
          new JsonResponseFormatter<GetDirectQueryResourcesResponse>(
              JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(GetDirectQueryResourcesResponse response) {
              return response;
            }
          }.format(response);
      listener.onResponse(new ReadDirectQueryResourcesActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
