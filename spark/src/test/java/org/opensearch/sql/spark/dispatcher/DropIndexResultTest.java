/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DATA_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ERROR_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STATUS_FIELD;

import com.amazonaws.services.emrserverless.model.JobRunState;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class DropIndexResultTest {
  // todo, remove this UT after response refactor.
  @Test
  public void successRespEncodeDecode() {
    // encode jobId
    String jobId =
        new SparkQueryDispatcher.DropIndexResult(JobRunState.SUCCESS.toString(), "E").toJobId();

    // decode jobId
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(jobId);

    JSONObject result = dropIndexResult.result();
    assertEquals(JobRunState.SUCCESS.toString(), result.get(STATUS_FIELD));
    assertEquals(
        "{\"result\":[],\"schema\":[],\"applicationId\":\"fakeDropId\"}",
        result.get(DATA_FIELD).toString());
  }

  // todo, remove this UT after response refactor.
  @Test
  public void failedRespEncodeDecode() {
    // encode jobId
    String jobId =
        new SparkQueryDispatcher.DropIndexResult(JobRunState.FAILED.toString(), "error").toJobId();

    // decode jobId
    SparkQueryDispatcher.DropIndexResult dropIndexResult =
        SparkQueryDispatcher.DropIndexResult.fromJobId(jobId);

    JSONObject result = dropIndexResult.result();
    assertEquals(JobRunState.FAILED.toString(), result.get(STATUS_FIELD));
    assertEquals("error", result.get(ERROR_FIELD));
  }
}
