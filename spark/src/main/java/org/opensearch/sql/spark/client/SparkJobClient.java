/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;

public interface SparkJobClient {

  String startJobRun(
      String query,
      String jobName,
      String applicationId,
      String executionRoleArn,
      String sparkSubmitParams);

  GetJobRunResult getJobRunResult(String applicationId, String jobId);

  CancelJobRunResult cancelJobRun(String applicationId, String jobId);
}
