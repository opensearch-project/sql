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
import org.opensearch.sql.spark.transport.model.GetJobActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetJobRequestAction
    extends HandledTransportAction<GetJobActionRequest, GetJobActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/read";
  public static final ActionType<GetJobActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetJobActionResponse::new);

  @Inject
  public TransportGetJobRequestAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, GetJobActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, GetJobActionRequest request, ActionListener<GetJobActionResponse> listener) {
    String responseContent;
    if (request.getJobId() == null) {
      responseContent = handleGetAllJobs();
    } else {
      responseContent = handleGetJob(request.getJobId());
    }
    listener.onResponse(new GetJobActionResponse(responseContent));
  }

  private String handleGetAllJobs() {
    return "All Jobs Information.";
  }

  private String handleGetJob(String jobId) {
    return String.format("Job %s details.", jobId);
  }
}
