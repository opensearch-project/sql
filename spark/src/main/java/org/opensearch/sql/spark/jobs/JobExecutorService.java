/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.jobs;

import org.opensearch.sql.spark.jobs.model.JobExecutionResponse;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.rest.model.CreateJobResponse;

/** JobExecutorService exposes functionality to create job, cancel job and get results of a job. */
public interface JobExecutorService {

  /**
   * Creates job based on the request and returns jobId in the response.
   *
   * @param createJobRequest createJobRequest.
   * @return {@link CreateJobResponse}
   */
  CreateJobResponse createJob(CreateJobRequest createJobRequest);

  /**
   * Returns job execution response for a given jobId.
   *
   * @param jobId jobId.
   * @return {@link JobExecutionResponse}
   */
  JobExecutionResponse getJobResults(String jobId);
}
