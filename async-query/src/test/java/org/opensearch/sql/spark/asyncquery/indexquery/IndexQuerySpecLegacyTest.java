/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.indexquery;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.ValidationException;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.MockFlintIndex;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.leasemanager.ConcurrencyLimitExceededException;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexQuerySpecLegacyTest extends IndexQuerySpecBase {
  /**
   * Happy case. expectation is
   *
   * <p>(1) Drop Index response is SUCCESS
   */
  @Test
  public void legacyBasicDropAndFetchAndCancel() {
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING, LEGACY_SPECIAL_CHARACTERS)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);

              // Mock flint index
              mockDS.createIndex();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              assertNotNull(response.getQueryId());
              assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

              // 2.fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(
                      response.getQueryId(), asyncQueryRequestContext);
              assertEquals("SUCCESS", asyncQueryResults.getStatus());
              assertNull(asyncQueryResults.getError());
              emrsClient.cancelJobRunCalled(1);

              // 3.cancel
              IllegalArgumentException exception =
                  assertThrows(
                      IllegalArgumentException.class,
                      () ->
                          asyncQueryExecutorService.cancelQuery(
                              response.getQueryId(), asyncQueryRequestContext));
              assertEquals("can't cancel index DML query", exception.getMessage());
            });
  }

  /**
   * Legacy Test, without state index support. Not EMR-S job running. expectation is
   *
   * <p>(1) Drop Index response is SUCCESS
   */
  @Test
  public void legacyDropIndexNoJobRunning() {
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING, LEGACY_MV, LEGACY_SPECIAL_CHARACTERS)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(
                        String applicationId, String jobId, boolean allowExceptionPropagation) {
                      throw new ValidationException("Job run is not in a cancellable state");
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);

              // Mock flint index
              mockDS.createIndex();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2.fetch result.
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(
                      response.getQueryId(), asyncQueryRequestContext);
              assertEquals("SUCCESS", asyncQueryResults.getStatus());
              assertNull(asyncQueryResults.getError());
            });
  }

  /**
   * Legacy Test, without state index support. Cancel EMR-S job call timeout. expectation is
   *
   * <p>(1) Drop Index response is FAILED
   */
  @Test
  public void legacyDropIndexCancelJobTimeout() {
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING, LEGACY_MV, LEGACY_SPECIAL_CHARACTERS)
        .forEach(
            mockDS -> {
              // Mock EMR-S always return running.
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      return new GetJobRunResult().withJobRun(new JobRun().withState("Running"));
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);

              // Mock flint index
              mockDS.createIndex();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(
                      response.getQueryId(), asyncQueryRequestContext);
              assertEquals("FAILED", asyncQueryResults.getStatus());
              assertEquals("Cancel job operation timed out.", asyncQueryResults.getError());
            });
  }

  /**
   * Legacy Test, without state index support. Not EMR-S job running. expectation is
   *
   * <p>(1) Drop Index response is SUCCESS
   */
  @Test
  public void legacyDropIndexSpecialCharacter() {
    FlintDatasetMock mockDS = LEGACY_SPECIAL_CHARACTERS;
    LocalEMRSClient emrsClient =
        new LocalEMRSClient() {
          @Override
          public CancelJobRunResult cancelJobRun(
              String applicationId, String jobId, boolean allowExceptionPropagation) {
            throw new ValidationException("Job run is not in a cancellable state");
          }
        };
    EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // Mock flint index
    mockDS.createIndex();

    // 1.drop index
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(mockDS.query, MYGLUE_DATASOURCE, LangType.SQL, null),
            asyncQueryRequestContext);

    // 2.fetch result.
    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(
            response.getQueryId(), asyncQueryRequestContext);
    assertEquals("SUCCESS", asyncQueryResults.getStatus());
    assertNull(asyncQueryResults.getError());
  }
}
