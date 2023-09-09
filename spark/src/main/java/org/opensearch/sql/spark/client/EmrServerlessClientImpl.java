/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_RESPONSE_BUFFER_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_APPLICATION_JAR;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EmrServerlessClientImpl implements EmrServerlessClient {

  private final AWSEMRServerless emrServerless;
  private static final Logger logger = LogManager.getLogger(EmrServerlessClientImpl.class);

  public EmrServerlessClientImpl(AWSEMRServerless emrServerless) {
    this.emrServerless = emrServerless;
  }

  @Override
  public String startJobRun(
      String query,
      String jobName,
      String applicationId,
      String executionRoleArn,
      String sparkSubmitParams) {
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(jobName)
            .withApplicationId(applicationId)
            .withExecutionRoleArn(executionRoleArn)
            .withJobDriver(
                new JobDriver()
                    .withSparkSubmit(
                        new SparkSubmit()
                            .withEntryPoint(SPARK_SQL_APPLICATION_JAR)
                            .withEntryPointArguments(query, SPARK_RESPONSE_BUFFER_INDEX_NAME)
                            .withSparkSubmitParameters(sparkSubmitParams)));
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
}
