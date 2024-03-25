/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;

/**
 * Client Interface for spark Job Submissions. Can have multiple implementations based on the
 * underlying spark infrastructure. Currently, we have one for EMRServerless {@link
 * EmrServerlessClientImpl}
 */
public interface EMRServerlessClient {

  /**
   * Start a new job run.
   *
   * @param startJobRequest startJobRequest
   * @return jobId.
   */
  String startJobRun(StartJobRequest startJobRequest);

  /**
   * Get status of emr serverless job run..
   *
   * @param applicationId serverless applicationId
   * @param jobId jobId.
   * @return {@link GetJobRunResult}
   */
  GetJobRunResult getJobRunResult(String applicationId, String jobId);

  /**
   * Cancel emr serverless job run.
   *
   * @param applicationId applicationId.
   * @param jobId jobId.
   * @return {@link CancelJobRunResult}
   */
  CancelJobRunResult cancelJobRun(
      String applicationId, String jobId, boolean allowExceptionPropagation);
}
