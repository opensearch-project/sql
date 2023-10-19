/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_RESPONSE_BUFFER_INDEX_NAME;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import com.amazonaws.services.emrserverless.model.ValidationException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EmrServerlessClientImpl implements EMRServerlessClient {

  private final AWSEMRServerless emrServerless;
  private static final Logger logger = LogManager.getLogger(EmrServerlessClientImpl.class);

  public EmrServerlessClientImpl(AWSEMRServerless emrServerless) {
    this.emrServerless = emrServerless;
  }

  @Override
  public String startJobRun(StartJobRequest startJobRequest) {
    String resultIndex =
        startJobRequest.getResultIndex() == null
            ? SPARK_RESPONSE_BUFFER_INDEX_NAME
            : startJobRequest.getResultIndex();
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(startJobRequest.getJobName())
            .withApplicationId(startJobRequest.getApplicationId())
            .withExecutionRoleArn(startJobRequest.getExecutionRoleArn())
            .withTags(startJobRequest.getTags())
            .withExecutionTimeoutMinutes(startJobRequest.executionTimeout())
            .withJobDriver(
                new JobDriver()
                    .withSparkSubmit(
                        new SparkSubmit()
                            .withEntryPoint(
                                "s3://flint-data-dp-eu-west-1-beta/code/flint/sql-job-assembly-0.1.0-SNAPSHOT.jar")
                            .withEntryPointArguments(startJobRequest.getQuery(), resultIndex)
                            .withSparkSubmitParameters(startJobRequest.getSparkSubmitParams())));
    StartJobRunResult startJobRunResult =
        AccessController.doPrivileged(
            (PrivilegedAction<StartJobRunResult>) () -> emrServerless.startJobRun(request));
    logger.info("Job Run ID: " + startJobRunResult.getJobRunId());
    return startJobRunResult.getJobRunId();
  }

  @Override
  public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
    GetJobRunRequest request =
        new GetJobRunRequest().withApplicationId(applicationId).withJobRunId(jobId);
    GetJobRunResult getJobRunResult =
        AccessController.doPrivileged(
            (PrivilegedAction<GetJobRunResult>) () -> emrServerless.getJobRun(request));
    logger.info("Job Run state: " + getJobRunResult.getJobRun().getState());
    return getJobRunResult;
  }

  @Override
  public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
    CancelJobRunRequest cancelJobRunRequest =
        new CancelJobRunRequest().withJobRunId(jobId).withApplicationId(applicationId);
    try {
      CancelJobRunResult cancelJobRunResult =
          AccessController.doPrivileged(
              (PrivilegedAction<CancelJobRunResult>)
                  () -> emrServerless.cancelJobRun(cancelJobRunRequest));
      logger.info(String.format("Job : %s cancelled", cancelJobRunResult.getJobRunId()));
      return cancelJobRunResult;
    } catch (ValidationException e) {
      throw new IllegalArgumentException(
          String.format("Couldn't cancel the queryId: %s due to %s", jobId, e.getMessage()));
    }
  }
}
