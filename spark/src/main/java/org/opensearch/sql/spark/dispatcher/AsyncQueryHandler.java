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

/** Process async query request. */
public abstract class AsyncQueryHandler {

  public JSONObject getQueryResponse(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    JSONObject result = getResponseFromResultIndex(asyncQueryJobMetadata);
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
      return getResponseFromExecutor(asyncQueryJobMetadata);
    }
  }

  protected abstract JSONObject getResponseFromResultIndex(
      AsyncQueryJobMetadata asyncQueryJobMetadata);

  protected abstract JSONObject getResponseFromExecutor(
      AsyncQueryJobMetadata asyncQueryJobMetadata);

  public abstract String cancelJob(AsyncQueryJobMetadata asyncQueryJobMetadata);
}
