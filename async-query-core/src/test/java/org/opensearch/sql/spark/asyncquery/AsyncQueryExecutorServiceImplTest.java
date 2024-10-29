/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class AsyncQueryExecutorServiceImplTest {

  private static final String QUERY = "select * from my_glue.default.http_logs";
  private static final String QUERY_ID = "QUERY_ID";

  @Mock private SparkQueryDispatcher sparkQueryDispatcher;
  @Mock private AsyncQueryJobMetadataStorageService asyncQueryJobMetadataStorageService;
  private AsyncQueryExecutorService jobExecutorService;

  @Mock private SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;
  @Mock private SparkSubmitParameterModifier sparkSubmitParameterModifier;
  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;

  @Captor private ArgumentCaptor<AsyncQueryJobMetadata> asyncQueryJobMetadataCaptor;

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
        new CreateAsyncQueryRequest(QUERY, "my_glue", LangType.SQL);
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(
            SparkExecutionEngineConfig.builder()
                .applicationId(EMRS_APPLICATION_ID)
                .region("eu-west-1")
                .executionRoleARN(EMRS_EXECUTION_ROLE)
                .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
                .clusterName(TEST_CLUSTER_NAME)
                .build());
    DispatchQueryRequest expectedDispatchQueryRequest =
        DispatchQueryRequest.builder()
            .applicationId(EMRS_APPLICATION_ID)
            .query(QUERY)
            .datasource("my_glue")
            .langType(LangType.SQL)
            .executionRoleARN(EMRS_EXECUTION_ROLE)
            .clusterName(TEST_CLUSTER_NAME)
            .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
            .build();
    when(sparkQueryDispatcher.dispatch(expectedDispatchQueryRequest, asyncQueryRequestContext))
        .thenReturn(
            DispatchQueryResponse.builder()
                .queryId(QUERY_ID)
                .jobId(EMR_JOB_ID)
                .jobType(JobType.INTERACTIVE)
                .build());

    CreateAsyncQueryResponse createAsyncQueryResponse =
        jobExecutorService.createAsyncQuery(createAsyncQueryRequest, asyncQueryRequestContext);

    verify(asyncQueryJobMetadataStorageService, times(1))
        .storeJobMetadata(getAsyncQueryJobMetadata(), asyncQueryRequestContext);
    verify(sparkExecutionEngineConfigSupplier, times(1))
        .getSparkExecutionEngineConfig(asyncQueryRequestContext);
    verify(sparkExecutionEngineConfigSupplier, times(1))
        .getSparkExecutionEngineConfig(asyncQueryRequestContext);
    verify(sparkQueryDispatcher, times(1))
        .dispatch(expectedDispatchQueryRequest, asyncQueryRequestContext);
    assertEquals(QUERY_ID, createAsyncQueryResponse.getQueryId());
  }

  @Test
  void testCreateAsyncQueryWithExtraSparkSubmitParameter() {
    SparkSubmitParameterModifier modifier =
        (builder) -> builder.extraParameters("--conf spark.dynamicAllocation.enabled=false");
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(
            SparkExecutionEngineConfig.builder()
                .applicationId(EMRS_APPLICATION_ID)
                .region("eu-west-1")
                .executionRoleARN(EMRS_EXECUTION_ROLE)
                .sparkSubmitParameterModifier(sparkSubmitParameterModifier)
                .sparkSubmitParameterModifier(modifier)
                .clusterName(TEST_CLUSTER_NAME)
                .build());
    when(sparkQueryDispatcher.dispatch(any(), any()))
        .thenReturn(
            DispatchQueryResponse.builder()
                .queryId(QUERY_ID)
                .jobId(EMR_JOB_ID)
                .jobType(JobType.INTERACTIVE)
                .build());

    jobExecutorService.createAsyncQuery(
        new CreateAsyncQueryRequest(QUERY, "my_glue", LangType.SQL), asyncQueryRequestContext);

    verify(sparkQueryDispatcher, times(1))
        .dispatch(
            argThat(actualReq -> actualReq.getSparkSubmitParameterModifier().equals(modifier)),
            eq(asyncQueryRequestContext));
  }

  @Test
  void testGetAsyncQueryResultsWithJobNotFoundException() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());

    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> jobExecutorService.getAsyncQueryResults(EMR_JOB_ID, asyncQueryRequestContext));

    assertEquals("QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testGetAsyncQueryResultsWithInProgressJob() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(getAsyncQueryJobMetadata()));
    JSONObject jobResult = new JSONObject();
    jobResult.put("status", JobRunState.PENDING.toString());
    when(sparkQueryDispatcher.getQueryResponse(
            getAsyncQueryJobMetadata(), asyncQueryRequestContext))
        .thenReturn(jobResult);

    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        jobExecutorService.getAsyncQueryResults(EMR_JOB_ID, asyncQueryRequestContext);

    Assertions.assertNull(asyncQueryExecutionResponse.getResults());
    Assertions.assertNull(asyncQueryExecutionResponse.getSchema());
    assertEquals("PENDING", asyncQueryExecutionResponse.getStatus());
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testGetAsyncQueryResultsWithSuccessJob() throws IOException {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(getAsyncQueryJobMetadata()));
    JSONObject jobResult = new JSONObject(getJson("select_query_response.json"));
    jobResult.put("status", JobRunState.SUCCESS.toString());
    when(sparkQueryDispatcher.getQueryResponse(
            getAsyncQueryJobMetadata(), asyncQueryRequestContext))
        .thenReturn(jobResult);

    AsyncQueryExecutionResponse asyncQueryExecutionResponse =
        jobExecutorService.getAsyncQueryResults(EMR_JOB_ID, asyncQueryRequestContext);

    assertEquals("SUCCESS", asyncQueryExecutionResponse.getStatus());
    assertEquals(1, asyncQueryExecutionResponse.getSchema().getColumns().size());
    assertEquals("1", asyncQueryExecutionResponse.getSchema().getColumns().get(0).getName());
    assertEquals(
        1,
        ((HashMap<String, String>) asyncQueryExecutionResponse.getResults().get(0).value())
            .get("1"));
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testCancelJobWithJobNotFound() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.empty());

    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> jobExecutorService.cancelQuery(EMR_JOB_ID, asyncQueryRequestContext));

    assertEquals("QueryId: " + EMR_JOB_ID + " not found", asyncQueryNotFoundException.getMessage());
    verifyNoInteractions(sparkQueryDispatcher);
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  @Test
  void testCancelJob() {
    when(asyncQueryJobMetadataStorageService.getJobMetadata(EMR_JOB_ID))
        .thenReturn(Optional.of(getAsyncQueryJobMetadata()));
    when(sparkQueryDispatcher.cancelJob(getAsyncQueryJobMetadata(), asyncQueryRequestContext))
        .thenReturn(EMR_JOB_ID);

    String jobId = jobExecutorService.cancelQuery(EMR_JOB_ID, asyncQueryRequestContext);

    assertEquals(EMR_JOB_ID, jobId);
    verify(asyncQueryJobMetadataStorageService)
        .updateState(any(), eq(QueryState.CANCELLED), eq(asyncQueryRequestContext));
    verifyNoInteractions(sparkExecutionEngineConfigSupplier);
  }

  private AsyncQueryJobMetadata getAsyncQueryJobMetadata() {
    return AsyncQueryJobMetadata.builder()
        .queryId(QUERY_ID)
        .applicationId(EMRS_APPLICATION_ID)
        .jobId(EMR_JOB_ID)
        .query(QUERY)
        .langType(LangType.SQL)
        .build();
  }
}
