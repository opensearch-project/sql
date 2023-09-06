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
import org.opensearch.sql.spark.transport.model.CreateJobActionRequest;
import org.opensearch.sql.spark.transport.model.CreateJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCreateJobRequestAction
    extends HandledTransportAction<CreateJobActionRequest, CreateJobActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/create";
  public static final ActionType<CreateJobActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CreateJobActionResponse::new);

  @Inject
  public TransportCreateJobRequestAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, CreateJobActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, CreateJobActionRequest request, ActionListener<CreateJobActionResponse> listener) {
    String responseContent = "submitted_job";
    listener.onResponse(new CreateJobActionResponse(responseContent));
  }
}
