/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.asyncquery.OpensearchAsyncQueryAsyncQueryJobMetadataStorageServiceTest.DS_NAME;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class AsyncQueryExecutorServiceImplTest {

  @Mock private SparkQueryDispatcher sparkQueryDispatcher;
  @Mock private AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  private AsyncQueryExecutorService jobExecutorService;

  @Mock private SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;
  private final AsyncQueryId QUERY_ID = AsyncQueryId.newAsyncQueryId(DS_NAME);

  @BeforeEach
  void setUp() {
    jobExecutorService =
        new AsyncQueryExecutorServiceImpl(
            asyncQueryJobMetadataStorageService,
            sparkQueryDispatcher,
            sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testCreateAsyncQuery() {
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest(
            "select * from my_glue.default.http_logs", "my_glue", LangType.SQL);
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig())
        .thenReturn(
            new SparkExecutionEngineConfig(
                "00fd775baqpu4g0p",
                "eu-west-1",
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                null,
                TEST_CLUSTER_NAME));
    when(sparkQueryDispatcher.dispatch(
            new DispatchQueryRequest(
                "00fd775baqpu4g0p",
                "select * from my_glue.default.http_logs",
                "my_glue",
                LangType.SQL,
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                TEST_CLUSTER_NAME)))
        .thenReturn(new DispatchQueryResponse(QUERY_ID, EMR_JOB_ID, false, null, null));
    CreateAsyncQueryResponse createAsyncQueryResponse =
        jobExecutorService.createAsyncQuery(createAsyncQueryRequest);
    verify(asyncQueryJobMetadataStorageService, times(1))
        .storeJobMetadata(
            new AsyncQueryJobMetadata(QUERY_ID, "00fd775baqpu4g0p", EMR_JOB_ID, null));
    verify(sparkExecutionEngineConfigSupplier, times(1)).getSparkExecutionEngineConfig();
    verify(sparkQueryDispatcher, times(1))
        .dispatch(
            new DispatchQueryRequest(
                "00fd775baqpu4g0p",
                "select * from my_glue.default.http_logs",
                "my_glue",
                LangType.SQL,
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                TEST_CLUSTER_NAME));
    Assertions.assertEquals(QUERY_ID.getId(), createAsyncQueryResponse.getQueryId());
  }

  @Test
  void testCreateAsyncQueryWithExtraSparkSubmitParameter() {
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig())
        .thenReturn(
            new SparkExecutionEngineConfig(
                "00fd775baqpu4g0p",
                "eu-west-1",
                "arn:aws:iam::270824043731:role/emr-job-execution-role",
                "--conf spark.dynamicAllocation.enabled=false",
                TEST_CLUSTER_NAME));
    when(sparkQueryDispatcher.dispatch(any()))
        .thenReturn(new DispatchQueryResponse(QUERY_ID, EMR_JOB_ID, false, null, null));

    jobExecutorService.createAsyncQuery(
        new CreateAsyncQueryRequest(
            "select * from my_glue.default.http_logs", "my_glue", LangType.SQL));

    verify(sparkQueryDispatcher, times(1))
        .dispatch(
            argThat(
                actualReq ->
                    actualReq
                        .getExtraSparkSubmitParams()
                        .equals("--conf spark.dynamicAllocation.enabled=false")));
  }

  @Test
  void testGetAsyncQueryResultsWithJobNotFoundException() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> jobExecutorService.getAsyncQueryResults(EMR_JOB_ID));
    Assertions.assertEquals(
        "QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testGetAsyncQueryResultsWithInProgressJob() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(
            Optional.of(
                new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)));
    JSONObject jobResult = new JSONObject();
    jobResult.put("status", JobRunState.PENDING.toString());
    when(sparkQueryDispatcher.getQueryResponse(
            new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)))
        .thenReturn(jobResult);
    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        jobExecutorService.getAsyncQueryResults(EMR_JOB_ID);

    Assertions.assertNull(asyncQueryExecutionResponse.getResults());
    Assertions.assertNull(asyncQueryExecutionResponse.getSchema());
    Assertions.assertEquals("PENDING", asyncQueryExecutionResponse.getStatus());
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testGetAsyncQueryResultsWithSuccessJob() throws IOException {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(
            Optional.of(
                new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)));
    JSONObject jobResult = new JSONObject(getJson("select_query_response.json"));
    jobResult.put("status", JobRunState.SUCCESS.toString());
    when(sparkQueryDispatcher.getQueryResponse(
            new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)))
        .thenReturn(jobResult);

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
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
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
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class, () -> jobExecutorService.cancelQuery(EMR_JOB_ID));
    Assertions.assertEquals(
        "QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testCancelJob() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(
            Optional.of(
                new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)));
    when(sparkQueryDispatcher.cancelJob(
            new AsyncQueryJobMetadata(QUERY_ID, EMRS_APPLICATION_ID, EMR_JOB_ID, null)))
        .thenReturn(EMR_JOB_ID);
    String jobId = jobExecutorService.cancelQuery(EMR_JOB_ID);
    Assertions.assertEquals(EMR_JOB_ID, jobId);
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }
}
