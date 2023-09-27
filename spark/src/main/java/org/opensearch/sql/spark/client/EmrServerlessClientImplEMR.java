/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_RESPONSE_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_APPLICATION_JAR;

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

public class EmrServerlessClientImplEMR implements EMRServerlessClient {

  private final AWSEMRServerless emrServerless;
  private static final Logger logger = LogManager.getLogger(EmrServerlessClientImplEMR.class);

  public EmrServerlessClientImplEMR(AWSEMRServerless emrServerless) {
    this.emrServerless = emrServerless;
  }

  @Override
  public String startJobRun(StartJobRequest startJobRequest) {
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(startJobRequest.getJobName())
            .withApplicationId(startJobRequest.getApplicationId())
            .withExecutionRoleArn(startJobRequest.getExecutionRoleArn())
            .withTags(startJobRequest.getTags())
            .withJobDriver(
                new JobDriver()
                    .withSparkSubmit(
                        new SparkSubmit()
                            .withEntryPoint(SPARK_SQL_APPLICATION_JAR)
                            .withEntryPointArguments(
                                startJobRequest.getQuery(), SPARK_RESPONSE_BUFFER_INDEX_NAME)
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
