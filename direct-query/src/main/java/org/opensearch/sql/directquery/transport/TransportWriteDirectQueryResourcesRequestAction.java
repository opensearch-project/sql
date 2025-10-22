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
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.WriteDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.WriteDirectQueryResourcesActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/*
 * @opensearch.experimental
 */
public class TransportWriteDirectQueryResourcesRequestAction
    extends HandledTransportAction<
        WriteDirectQueryResourcesActionRequest, WriteDirectQueryResourcesActionResponse> {

  private final DirectQueryExecutorService directQueryExecutorService;

  public static final String NAME = "cluster:admin/opensearch/direct_query/write/resources";
  public static final ActionType<WriteDirectQueryResourcesActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, WriteDirectQueryResourcesActionResponse::new);

  @Inject
  public TransportWriteDirectQueryResourcesRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      DirectQueryExecutorServiceImpl directQueryExecutorService) {
    super(NAME, transportService, actionFilters, WriteDirectQueryResourcesActionRequest::new);
    this.directQueryExecutorService = (DirectQueryExecutorService) directQueryExecutorService;
  }

  @Override
  protected void doExecute(
      Task task,
      WriteDirectQueryResourcesActionRequest request,
      ActionListener<WriteDirectQueryResourcesActionResponse> listener) {
    try {
      WriteDirectQueryResourcesRequest directQueryRequest = request.getDirectQueryRequest();

      WriteDirectQueryResourcesResponse response =
          directQueryExecutorService.writeDirectQueryResources(directQueryRequest);
      String responseContent =
          new JsonResponseFormatter<WriteDirectQueryResourcesResponse>(
              JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(WriteDirectQueryResourcesResponse response) {
              return response;
            }
          }.format(response);
      listener.onResponse(new WriteDirectQueryResourcesActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
