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
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetQueryResultRequestAction
    extends HandledTransportAction<
        GetJobQueryResultActionRequest, GetJobQueryResultActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/result";
  public static final ActionType<GetJobQueryResultActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetJobQueryResultActionResponse::new);

  @Inject
  public TransportGetQueryResultRequestAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, GetJobQueryResultActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      GetJobQueryResultActionRequest request,
      ActionListener<GetJobQueryResultActionResponse> listener) {
    String responseContent = "job result";
    listener.onResponse(new GetJobQueryResultActionResponse(responseContent));
  }
}
