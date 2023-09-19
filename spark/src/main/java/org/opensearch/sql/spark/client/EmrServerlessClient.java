/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.model.GetJobRunResult;

public interface EmrServerlessClient {

  String startJobRun(
      String query,
      String jobName,
      String applicationId,
      String executionRoleArn,
      String sparkSubmitParams);

  GetJobRunResult getJobRunResult(String applicationId, String jobId);
}
