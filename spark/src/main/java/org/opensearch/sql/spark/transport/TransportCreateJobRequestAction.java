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
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.spark.jobs.JobExecutorService;
import org.opensearch.sql.spark.jobs.JobExecutorServiceImpl;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.rest.model.CreateJobResponse;
import org.opensearch.sql.spark.transport.model.CreateJobActionRequest;
import org.opensearch.sql.spark.transport.model.CreateJobActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCreateJobRequestAction
    extends HandledTransportAction<CreateJobActionRequest, CreateJobActionResponse> {

  private final JobExecutorService jobExecutorService;

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/create";
  public static final ActionType<CreateJobActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CreateJobActionResponse::new);

  @Inject
  public TransportCreateJobRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      JobExecutorServiceImpl jobManagementService) {
    super(NAME, transportService, actionFilters, CreateJobActionRequest::new);
    this.jobExecutorService = jobManagementService;
  }

  @Override
  protected void doExecute(
      Task task, CreateJobActionRequest request, ActionListener<CreateJobActionResponse> listener) {
    try {
      CreateJobRequest createJobRequest = request.getCreateJobRequest();
      CreateJobResponse createJobResponse = jobExecutorService.createJob(createJobRequest);
      String responseContent =
          new JsonResponseFormatter<CreateJobResponse>(JsonResponseFormatter.Style.PRETTY) {
            @Override
            protected Object buildJsonObject(CreateJobResponse response) {
              return response;
            }
          }.format(createJobResponse);
      listener.onResponse(new CreateJobActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
