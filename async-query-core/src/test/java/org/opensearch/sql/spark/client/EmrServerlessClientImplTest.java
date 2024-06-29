/* Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.DEFAULT_RESULT_INDEX;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_JOB_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.ENTRY_POINT_START_JAR;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;
import static org.opensearch.sql.spark.constants.TestConstants.SPARK_SUBMIT_PARAMETERS;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.AWSEMRServerlessException;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import com.amazonaws.services.emrserverless.model.ValidationException;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.metrics.MetricsService;
import org.opensearch.sql.spark.parameter.SparkParameterComposerCollection;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilder;

@ExtendWith(MockitoExtension.class)
public class EmrServerlessClientImplTest {
  @Mock private AWSEMRServerless emrServerless;
  @Mock private MetricsService metricsService;

  @Captor private ArgumentCaptor<StartJobRunRequest> startJobRunRequestArgumentCaptor;

  @InjectMocks EmrServerlessClientImpl emrServerlessClient;

  @Test
  void testStartJobRun() {
    StartJobRunResult response = new StartJobRunResult();
    when(emrServerless.startJobRun(any())).thenReturn(response);

    String parameters =
        new SparkSubmitParametersBuilder(new SparkParameterComposerCollection())
            .query(QUERY)
            .toString();

    emrServerlessClient.startJobRun(
        new StartJobRequest(
            EMRS_JOB_NAME,
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            parameters,
            new HashMap<>(),
            false,
            DEFAULT_RESULT_INDEX));
    verify(emrServerless, times(1)).startJobRun(startJobRunRequestArgumentCaptor.capture());
    StartJobRunRequest startJobRunRequest = startJobRunRequestArgumentCaptor.getValue();
    Assertions.assertEquals(EMRS_APPLICATION_ID, startJobRunRequest.getApplicationId());
    Assertions.assertEquals(EMRS_EXECUTION_ROLE, startJobRunRequest.getExecutionRoleArn());
    Assertions.assertEquals(
        ENTRY_POINT_START_JAR, startJobRunRequest.getJobDriver().getSparkSubmit().getEntryPoint());
    Assertions.assertEquals(
        List.of(DEFAULT_RESULT_INDEX),
        startJobRunRequest.getJobDriver().getSparkSubmit().getEntryPointArguments());
    Assertions.assertTrue(
        startJobRunRequest
            .getJobDriver()
            .getSparkSubmit()
            .getSparkSubmitParameters()
            .contains(QUERY));
  }

  @Test
  void testStartJobRunWithErrorMetric() {
    doThrow(new AWSEMRServerlessException("Couldn't start job"))
        .when(emrServerless)
        .startJobRun(any());
    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                emrServerlessClient.startJobRun(
                    new StartJobRequest(
                        EMRS_JOB_NAME,
                        null,
                        EMRS_APPLICATION_ID,
                        EMRS_EXECUTION_ROLE,
                        SPARK_SUBMIT_PARAMETERS,
                        new HashMap<>(),
                        false,
                        null)));
    Assertions.assertEquals("Internal Server Error.", runtimeException.getMessage());
  }

  @Test
  void testStartJobRunResultIndex() {
    StartJobRunResult response = new StartJobRunResult();
    when(emrServerless.startJobRun(any())).thenReturn(response);

    emrServerlessClient.startJobRun(
        new StartJobRequest(
            EMRS_JOB_NAME,
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            SPARK_SUBMIT_PARAMETERS,
            new HashMap<>(),
            false,
            "foo"));
  }

  @Test
  void testGetJobRunState() {
    JobRun jobRun = new JobRun();
    jobRun.setState("Running");
    GetJobRunResult response = new GetJobRunResult();
    response.setJobRun(jobRun);
    when(emrServerless.getJobRun(any())).thenReturn(response);
    emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, "123");
  }

  @Test
  void testGetJobRunStateWithErrorMetric() {
    doThrow(new ValidationException("Not a good job")).when(emrServerless).getJobRun(any());
    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, "123"));
    Assertions.assertEquals("Internal Server Error.", runtimeException.getMessage());
  }

  @Test
  void testCancelJobRun() {
    when(emrServerless.cancelJobRun(any()))
        .thenReturn(new CancelJobRunResult().withJobRunId(EMR_JOB_ID));

    CancelJobRunResult cancelJobRunResult =
        emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, false);

    Assertions.assertEquals(EMR_JOB_ID, cancelJobRunResult.getJobRunId());
  }

  @Test
  void testCancelJobRunWithErrorMetric() {
    doThrow(new RuntimeException()).when(emrServerless).cancelJobRun(any());

    Assertions.assertThrows(
        RuntimeException.class,
        () -> emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, "123", false));
  }

  @Test
  void testCancelJobRunWithValidationException() {
    doThrow(new ValidationException("Error")).when(emrServerless).cancelJobRun(any());

    RuntimeException runtimeException =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, false));

    Assertions.assertEquals("Internal Server Error.", runtimeException.getMessage());
  }

  @Test
  void testCancelJobRunWithNativeEMRExceptionWithValidationException() {
    doThrow(new ValidationException("Error")).when(emrServerless).cancelJobRun(any());

    ValidationException validationException =
        Assertions.assertThrows(
            ValidationException.class,
            () -> emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, true));

    Assertions.assertTrue(validationException.getMessage().contains("Error"));
  }

  @Test
  void testCancelJobRunWithNativeEMRException() {
    when(emrServerless.cancelJobRun(any()))
        .thenReturn(new CancelJobRunResult().withJobRunId(EMR_JOB_ID));

    CancelJobRunResult cancelJobRunResult =
        emrServerlessClient.cancelJobRun(EMRS_APPLICATION_ID, EMR_JOB_ID, true);

    Assertions.assertEquals(EMR_JOB_ID, cancelJobRunResult.getJobRunId());
  }

  @Test
  void testStartJobRunWithLongJobName() {
    StartJobRunResult response = new StartJobRunResult();
    when(emrServerless.startJobRun(any())).thenReturn(response);

    emrServerlessClient.startJobRun(
        new StartJobRequest(
            RandomStringUtils.random(300),
            null,
            EMRS_APPLICATION_ID,
            EMRS_EXECUTION_ROLE,
            SPARK_SUBMIT_PARAMETERS,
            new HashMap<>(),
            false,
            DEFAULT_RESULT_INDEX));

    verify(emrServerless, times(1)).startJobRun(startJobRunRequestArgumentCaptor.capture());
    StartJobRunRequest startJobRunRequest = startJobRunRequestArgumentCaptor.getValue();
    Assertions.assertEquals(255, startJobRunRequest.getName().length());
  }

  @Test
  void testStartJobRunThrowsValidationException() {
    when(emrServerless.startJobRun(any())).thenThrow(new ValidationException("Unmatched quote"));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                emrServerlessClient.startJobRun(
                    new StartJobRequest(
                        EMRS_JOB_NAME,
                        null,
                        EMRS_APPLICATION_ID,
                        EMRS_EXECUTION_ROLE,
                        SPARK_SUBMIT_PARAMETERS,
                        new HashMap<>(),
                        false,
                        DEFAULT_RESULT_INDEX)),
            "Expected ValidationException to be thrown");

    // Verify that the message in the exception is correct
    Assertions.assertEquals(
        "The input fails to satisfy the constraints specified by AWS EMR Serverless.",
        exception.getMessage());

    // Optionally verify that no job run is started
    verify(emrServerless, times(1)).startJobRun(any());
  }
}
