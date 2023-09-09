/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import org.json.JSONObject;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.spark.jobs.JobExecutorService;
import org.opensearch.sql.spark.jobs.JobExecutorServiceImpl;
import org.opensearch.sql.spark.jobs.model.JobExecutionResponse;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetQueryResultRequestAction
    extends HandledTransportAction<
        GetJobQueryResultActionRequest, GetJobQueryResultActionResponse> {

  private final JobExecutorService jobExecutorService;

  public static final String NAME = "cluster:admin/opensearch/ql/jobs/result";
  public static final ActionType<GetJobQueryResultActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetJobQueryResultActionResponse::new);

  @Inject
  public TransportGetQueryResultRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      JobExecutorServiceImpl jobManagementService) {
    super(NAME, transportService, actionFilters, GetJobQueryResultActionRequest::new);
    this.jobExecutorService = jobManagementService;
  }

  @Override
  protected void doExecute(
      Task task,
      GetJobQueryResultActionRequest request,
      ActionListener<GetJobQueryResultActionResponse> listener) {
    try {
      String jobId = request.getJobId();
      JobExecutionResponse jobExecutionResponse = jobExecutorService.getJobResults(jobId);
      if (!jobExecutionResponse.getStatus().equals("SUCCESS")) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("status", jobExecutionResponse.getStatus());
        listener.onResponse(new GetJobQueryResultActionResponse(jsonObject.toString()));
      } else {
        ResponseFormatter<QueryResult> formatter =
            new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
        String responseContent =
            formatter.format(
                new QueryResult(
                    jobExecutionResponse.getSchema(),
                    jobExecutionResponse.getResults(),
                    Cursor.None));
        listener.onResponse(new GetJobQueryResultActionResponse(responseContent));
      }
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
