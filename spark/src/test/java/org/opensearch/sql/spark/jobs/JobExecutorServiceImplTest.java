/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.jobs;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.utils.TestUtils.getJson;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.jobs.exceptions.JobNotFoundException;
import org.opensearch.sql.spark.jobs.model.JobExecutionResponse;
import org.opensearch.sql.spark.jobs.model.JobMetadata;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.rest.model.CreateJobResponse;

@ExtendWith(MockitoExtension.class)
public class JobExecutorServiceImplTest {

  @Mock private SparkQueryDispatcher sparkQueryDispatcher;
  @Mock private JobMetadataStorageService jobMetadataStorageService;
  @Mock private Settings settings;

  @Test
  void testCreateJob() {
    JobExecutorServiceImpl jobExecutorService =
        new JobExecutorServiceImpl(jobMetadataStorageService, sparkQueryDispatcher, settings);
    CreateJobRequest createJobRequest =
        new CreateJobRequest("select * from my_glue.default.http_logs");
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG))
        .thenReturn(
            "{\"applicationId\":\"00fd775baqpu4g0p\",\"executionRoleARN\":\"arn:aws:iam::270824043731:role/emr-job-execution-role\",\"region\":\"eu-west-1\"}");
    when(sparkQueryDispatcher.dispatch(
            "00fd775baqpu4g0p",
            "select * from my_glue.default.http_logs",
            "arn:aws:iam::270824043731:role/emr-job-execution-role"))
        .thenReturn(EMR_JOB_ID);
    CreateJobResponse createJobResponse = jobExecutorService.createJob(createJobRequest);
    verify(jobMetadataStorageService, times(1))
        .storeJobMetadata(new JobMetadata(EMR_JOB_ID, "00fd775baqpu4g0p"));
    verify(settings, times(1)).getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG);
    verify(sparkQueryDispatcher, times(1))
        .dispatch(
            "00fd775baqpu4g0p",
            "select * from my_glue.default.http_logs",
            "arn:aws:iam::270824043731:role/emr-job-execution-role");
    Assertions.assertEquals(EMR_JOB_ID, createJobResponse.getJobId());
  }

  @Test
  void testGetJobResultsWithJobNotFoundException() {
    JobExecutorServiceImpl jobExecutorService =
        new JobExecutorServiceImpl(jobMetadataStorageService, sparkQueryDispatcher, settings);
    when(jobMetadataStorageService.getJobMetadata(EMR_JOB_ID)).thenReturn(Optional.empty());
    JobNotFoundException jobNotFoundException =
        Assertions.assertThrows(
            JobNotFoundException.class, () -> jobExecutorService.getJobResults(EMR_JOB_ID));
    Assertions.assertEquals(
        "JobId: " + EMR_JOB_ID + " not found", jobNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(settings);
  }

  @Test
  void testGetJobResultsWithInProgressJob() {
    JobExecutorServiceImpl jobExecutorService =
        new JobExecutorServiceImpl(jobMetadataStorageService, sparkQueryDispatcher, settings);
    when(jobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(new JobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID)));
    JSONObject jobResult = new JSONObject();
    jobResult.put("status", JobRunState.PENDING.toString());
    when(sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(jobResult);
    JobExecutionResponse jobExecutionResponse = jobExecutorService.getJobResults(EMR_JOB_ID);

    Assertions.assertNull(jobExecutionResponse.getResults());
    Assertions.assertNull(jobExecutionResponse.getSchema());
    Assertions.assertEquals("PENDING", jobExecutionResponse.getStatus());
    verifyNoInteractions(settings);
  }

  @Test
  void testGetJobResultsWithSuccessJob() throws IOException {
    when(jobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(new JobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID)));
    JSONObject jobResult = new JSONObject(getJson("select_query_response.json"));
    jobResult.put("status", JobRunState.SUCCESS.toString());
    when(sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(jobResult);

    JobExecutorServiceImpl jobExecutorService =
        new JobExecutorServiceImpl(jobMetadataStorageService, sparkQueryDispatcher, settings);
    JobExecutionResponse jobExecutionResponse = jobExecutorService.getJobResults(EMR_JOB_ID);

    Assertions.assertEquals("SUCCESS", jobExecutionResponse.getStatus());
    Assertions.assertEquals(1, jobExecutionResponse.getSchema().getColumns().size());
    Assertions.assertEquals("1", jobExecutionResponse.getSchema().getColumns().get(0).getName());
    Assertions.assertEquals(
        1, ((HashMap<String, String>) jobExecutionResponse.getResults().get(0).value()).get("1"));
    verifyNoInteractions(settings);
  }

  @Test
  void testGetJobResultsWithDisabledExecutionEngine() {
    JobExecutorService jobExecutorService = new JobExecutorServiceImpl();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> jobExecutorService.getJobResults(EMR_JOB_ID));
    Assertions.assertEquals(
        "Job APIs are disabled as plugins.query.executionengine.spark.config is not configured in"
            + " cluster settings. Please configure the setting and restart the domain to enable"
            + " JobAPIs",
        illegalArgumentException.getMessage());
  }
}
