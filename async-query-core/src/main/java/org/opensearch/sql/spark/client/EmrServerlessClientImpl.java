/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.datasource.model.DataSourceMetadata.DEFAULT_RESULT_INDEX;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;

public class EmrServerlessClientImpl implements EMRServerlessClient {

  private final AWSEMRServerless emrServerless;
  private static final Logger logger = LogManager.getLogger(EmrServerlessClientImpl.class);

  private static final int MAX_JOB_NAME_LENGTH = 255;

  public static final String GENERIC_INTERNAL_SERVER_ERROR_MESSAGE = "Internal Server Error.";

  public EmrServerlessClientImpl(AWSEMRServerless emrServerless) {
    this.emrServerless = emrServerless;
  }

  @Override
  public String startJobRun(StartJobRequest startJobRequest) {
    String resultIndex =
        startJobRequest.getResultIndex() == null
            ? DEFAULT_RESULT_INDEX
            : startJobRequest.getResultIndex();
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(StringUtils.truncate(startJobRequest.getJobName(), MAX_JOB_NAME_LENGTH))
            .withApplicationId(startJobRequest.getApplicationId())
            .withExecutionRoleArn(startJobRequest.getExecutionRoleArn())
            .withTags(startJobRequest.getTags())
            .withExecutionTimeoutMinutes(startJobRequest.executionTimeout())
            .withJobDriver(
                new JobDriver()
                    .withSparkSubmit(
                        new SparkSubmit()
                            .withEntryPoint(SPARK_SQL_APPLICATION_JAR)
                            .withEntryPointArguments(resultIndex)
                            .withSparkSubmitParameters(startJobRequest.getSparkSubmitParams())));

    StartJobRunResult startJobRunResult =
        AccessController.doPrivileged(
            (PrivilegedAction<StartJobRunResult>)
                () -> {
                  try {
                    return emrServerless.startJobRun(request);
                  } catch (Throwable t) {
                    logger.error("Error while making start job request to emr:", t);
                    MetricUtils.incrementNumericalMetric(
                        MetricName.EMR_START_JOB_REQUEST_FAILURE_COUNT);
                    if (t instanceof ValidationException) {
                      throw new IllegalArgumentException(
                          "The input fails to satisfy the constraints specified by AWS EMR"
                              + " Serverless.");
                    }
                    throw new RuntimeException(GENERIC_INTERNAL_SERVER_ERROR_MESSAGE);
                  }
                });
    logger.info("Job Run ID: " + startJobRunResult.getJobRunId());
    return startJobRunResult.getJobRunId();
  }

  @Override
  public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
    GetJobRunRequest request =
        new GetJobRunRequest().withApplicationId(applicationId).withJobRunId(jobId);
    GetJobRunResult getJobRunResult =
        AccessController.doPrivileged(
            (PrivilegedAction<GetJobRunResult>)
                () -> {
                  try {
                    return emrServerless.getJobRun(request);
                  } catch (Throwable t) {
                    logger.error("Error while making get job run request to emr:", t);
                    MetricUtils.incrementNumericalMetric(
                        MetricName.EMR_GET_JOB_RESULT_FAILURE_COUNT);
                    throw new RuntimeException(GENERIC_INTERNAL_SERVER_ERROR_MESSAGE);
                  }
                });
    logger.info("Job Run state: " + getJobRunResult.getJobRun().getState());
    return getJobRunResult;
  }

  @Override
  public CancelJobRunResult cancelJobRun(
      String applicationId, String jobId, boolean allowExceptionPropagation) {
    CancelJobRunRequest cancelJobRunRequest =
        new CancelJobRunRequest().withJobRunId(jobId).withApplicationId(applicationId);
    CancelJobRunResult cancelJobRunResult =
        AccessController.doPrivileged(
            (PrivilegedAction<CancelJobRunResult>)
                () -> {
                  try {
                    return emrServerless.cancelJobRun(cancelJobRunRequest);
                  } catch (Throwable t) {
                    if (allowExceptionPropagation) {
                      throw t;
                    } else {
                      logger.error("Error while making cancel job request to emr:", t);
                      MetricUtils.incrementNumericalMetric(
                          MetricName.EMR_CANCEL_JOB_REQUEST_FAILURE_COUNT);
                      throw new RuntimeException(GENERIC_INTERNAL_SERVER_ERROR_MESSAGE);
                    }
                  }
                });
    logger.info(String.format("Job : %s cancelled", cancelJobRunResult.getJobRunId()));
    return cancelJobRunResult;
  }
}
