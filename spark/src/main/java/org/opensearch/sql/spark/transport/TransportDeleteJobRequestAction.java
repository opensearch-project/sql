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
import org.opensearch.sql.spark.transport.model.DeleteJobActionRequest;
import org.opensearch.sql.spark.transport.model.DeleteJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportDeleteJobRequestAction
    extends HandledTransportAction<DeleteJobActionRequest, DeleteJobActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/delete";
  public static final ActionType<DeleteJobActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, DeleteJobActionResponse::new);

  @Inject
  public TransportDeleteJobRequestAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, DeleteJobActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, DeleteJobActionRequest request, ActionListener<DeleteJobActionResponse> listener) {
    String responseContent = "deleted_job";
    listener.onResponse(new DeleteJobActionResponse(responseContent));
  }
}
