/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
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
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class AsyncQueryExecutorServiceImplTest {

  @Mock private SparkQueryDispatcher sparkQueryDispatcher;
  @Mock private AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  @Mock private Settings settings;

  @Test
  void testCreateAsyncQuery() {
    AsyncQueryExecutorServiceImpl jobExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest("select * from my_glue.default.http_logs", LangType.SQL);
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG))
        .thenReturn(
            "{\"applicationId\":\"00fd775baqpu4g0p\",\"executionRoleARN\":\"arn:aws:iam::270824043731:role/emr-job-execution-role\",\"region\":\"eu-west-1\"}");
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));
    when(sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                "00fd775baqpu4g0p",
                "select * from my_glue.default.http_logs",
                LangType.SQL,
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                TEST_CLUSTER_NAME)))
        .thenReturn(EMR_JOB_ID);
    CreateAsyncQueryResponse createAsyncQueryResponse =
        jobExecutorService.createAsyncQuery(createAsyncQueryRequest);
    verify(asyncQueryJobMetadataStorageService, times(1))
        .storeJobMetadata(new AsyncQueryJobMetadata(EMR_JOB_ID, "00fd775baqpu4g0p"));
    verify(settings, times(1)).getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG);
    verify(settings, times(1)).getSettingValue(Settings.Key.CLUSTER_NAME);
    verify(sparkQueryDispatcher, times(1))
        .dispatch(
            new DispatchQueryRequest(
                "00fd775baqpu4g0p",
                "select * from my_glue.default.http_logs",
                LangType.SQL,
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                TEST_CLUSTER_NAME));
    Assertions.assertEquals(EMR_JOB_ID, createAsyncQueryResponse.getQueryId());
  }

  @Test
  void testGetAsyncQueryResultsWithJobNotFoundException() {
    AsyncQueryExecutorServiceImpl jobExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> jobExecutorService.getAsyncQueryResults(EMR_JOB_ID));
    Assertions.assertEquals(
        "QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(settings);
  }

  @Test
  void testGetAsyncQueryResultsWithInProgressJob() {
    AsyncQueryExecutorServiceImpl jobExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID)));
    JSONObject jobResult = new JSONObject();
    jobResult.put("status", JobRunState.PENDING.toString());
    when(sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(jobResult);
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        jobExecutorService.getAsyncQueryResults(EMR_JOB_ID);

    Assertions.assertNull(asyncQueryExecutionResponse.getResults());
    Assertions.assertNull(asyncQueryExecutionResponse.getSchema());
    Assertions.assertEquals("PENDING", asyncQueryExecutionResponse.getStatus());
    verifyNoInteractions(settings);
  }

  @Test
  void testGetAsyncQueryResultsWithSuccessJob() throws IOException {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID)));
    JSONObject jobResult = new JSONObject(getJson("select_query_response.json"));
    jobResult.put("status", JobRunState.SUCCESS.toString());
    when(sparkQueryDispatcher.getQueryResponse(EMRS_APPLICATION_ID, EMR_JOB_ID))
        .thenReturn(jobResult);

    AsyncQueryExecutorServiceImpl jobExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        jobExecutorService.getAsyncQueryResults(EMR_JOB_ID);

    Assertions.assertEquals("SUCCESS", asyncQueryExecutionResponse.getStatus());
    Assertions.assertEquals(1, asyncQueryExecutionResponse.getSchema().getColumns().size());
    Assertions.assertEquals(
        "1", asyncQueryExecutionResponse.getSchema().getColumns().get(0).getName());
    Assertions.assertEquals(
        1,
        ((HashMap<String, String>) asyncQueryExecutionResponse.getResults().get(0).value())
            .get("1"));
    verifyNoInteractions(settings);
  }

  @Test
  void testGetAsyncQueryResultsWithDisabledExecutionEngine() {
    AsyncQueryExecutorService asyncQueryExecutorService = new AsyncQueryExecutorServiceImpl();
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> asyncQueryExecutorService.getAsyncQueryResults(EMR_JOB_ID));
    Assertions.assertEquals(
        "Async Query APIs are disabled as plugins.query.executionengine.spark.config is not"
            + " configured in cluster settings. Please configure the setting and restart the domain"
            + " to enable Async Query APIs",
        illegalArgumentException.getMessage());
  }

  @Test
  void testCancelJobWithJobNotFound() {
    AsyncQueryExecutorService asyncQueryExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> asyncQueryExecutorService.cancelQuery(EMR_JOB_ID));
    Assertions.assertEquals(
        "QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(settings);
  }

  @Test
  void testCancelJob() {
    AsyncQueryExecutorService asyncQueryExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService, sparkQueryDispatcher, settings);
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(new AsyncQueryJobMetadata(EMR_JOB_ID, EMRS_APPLICATION_ID)));
    when(sparkQueryDispatcher.cancelJob(EMRS_APPLICATION_ID, EMR_JOB_ID)).thenReturn(EMR_JOB_ID);
    String jobId = asyncQueryExecutorService.cancelQuery(EMR_JOB_ID);
    Assertions.assertEquals(EMR_JOB_ID, jobId);
    verifyNoInteractions(settings);
  }
}
