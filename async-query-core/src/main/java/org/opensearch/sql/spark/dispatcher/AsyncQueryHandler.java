/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import org.json.JSONObject;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.execution.statement.StatementState;

/** Process async query request. */
public abstract class AsyncQueryHandler {

  public JSONObject getQueryResponse(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    JSONObject result = getResponseFromResultIndex(asyncQueryJobMetadata, asyncQueryRequestContext);
    if (result.has(DATA_FIELD)) {
      JSONObject items = result.getJSONObject(DATA_FIELD);

      // If items have STATUS_FIELD, use it; otherwise, mark failed
      String status = items.optString(STATUS_FIELD, JobRunState.FAILED.toString());
      result.put(STATUS_FIELD, status);

      // If items have ERROR_FIELD, use it; otherwise, set empty string
      String error = items.optString(ERROR_FIELD, "");
      result.put(ERROR_FIELD, error);
      return result;
    } else {
      JSONObject statement =
          getResponseFromExecutor(asyncQueryJobMetadata, asyncQueryRequestContext);

      // Consider statement still running if state is success but query result unavailable
      if (isSuccessState(statement)) {
        statement.put(STATUS_FIELD, StatementState.RUNNING.getState());
      }
      return statement;
    }
  }

  private boolean isSuccessState(JSONObject statement) {
    return StatementState.SUCCESS.getState().equalsIgnoreCase(statement.optString(STATUS_FIELD));
  }

  protected abstract JSONObject getResponseFromResultIndex(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext);

  protected abstract JSONObject getResponseFromExecutor(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext);

  public abstract String cancelJob(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext);

  public abstract DispatchQueryResponse submit(
      DispatchQueryRequest request, DispatchQueryContext context);
}
