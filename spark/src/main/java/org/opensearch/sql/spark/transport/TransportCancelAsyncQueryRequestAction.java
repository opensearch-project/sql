/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCancelAsyncQueryRequestAction
    extends HandledTransportAction<CancelAsyncQueryActionRequest, CancelAsyncQueryActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/delete";
  public static final ActionType<CancelAsyncQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CancelAsyncQueryActionResponse::new);

  @Inject
  public TransportCancelAsyncQueryRequestAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, CancelAsyncQueryActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      CancelAsyncQueryActionRequest request,
      ActionListener<CancelAsyncQueryActionResponse> listener) {
    String responseContent = "deleted_job";
    listener.onResponse(new CancelAsyncQueryActionResponse(responseContent));
  }
}
