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
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryResult;
import org.opensearch.sql.spark.transport.format.AsyncQueryResultResponseFormatter;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetAsyncQueryResultAction
    extends HandledTransportAction<
        GetAsyncQueryResultActionRequest, GetAsyncQueryResultActionResponse> {

  private final AsyncQueryExecutorService asyncQueryExecutorService;

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/result";
  public static final ActionType<GetAsyncQueryResultActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetAsyncQueryResultActionResponse::new);

  @Inject
  public TransportGetAsyncQueryResultAction(
      TransportService transportService,
      ActionFilters actionFilters,
      AsyncQueryExecutorServiceImpl jobManagementService) {
    super(NAME, transportService, actionFilters, GetAsyncQueryResultActionRequest::new);
    this.asyncQueryExecutorService = jobManagementService;
  }

  @Override
  protected void doExecute(
      Task task,
      GetAsyncQueryResultActionRequest request,
      ActionListener<GetAsyncQueryResultActionResponse> listener) {
    try {
      String jobId = request.getQueryId();
      AsyncQueryExecutionResponse asyncQueryExecutionResponse =
          asyncQueryExecutorService.getAsyncQueryResults(jobId);
      ResponseFormatter<AsyncQueryResult> formatter =
          new AsyncQueryResultResponseFormatter(JsonResponseFormatter.Style.PRETTY);
      String responseContent =
          formatter.format(
              new AsyncQueryResult(
                  asyncQueryExecutionResponse.getStatus(),
                  asyncQueryExecutionResponse.getSchema(),
                  asyncQueryExecutionResponse.getResults(),
                  Cursor.None));
      listener.onResponse(new GetAsyncQueryResultActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
