/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.indexquery;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
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

public class IndexQuerySpecCancelTest extends IndexQuerySpecBase {
  /** Cancel create flint index statement with auto_refresh=true, should throw exception. */
  @Test
  public void cancelAutoRefreshCreateFlintIndexShouldThrowException() {
    ImmutableList.of(CREATE_SI_AUTO, CREATE_CI_AUTO, CREATE_MV_AUTO)
        .forEach(
            query -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(
                        String applicationId, String jobId, boolean allowExceptionPropagation) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);

              // 1. submit create / refresh index query
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);

              // 2. cancel query
              IllegalArgumentException exception =
                  assertThrows(
                      IllegalArgumentException.class,
                      () ->
                          asyncQueryExecutorService.cancelQuery(
                              response.getQueryId(), asyncQueryRequestContext));
              assertEquals(
                  "can't cancel index DML query, using ALTER auto_refresh=off statement to stop"
                      + " job, using VACUUM statement to stop job and delete data",
                  exception.getMessage());
            });
  }

  /** Cancel REFRESH statement should success */
  @Test
  public void cancelRefreshStatement() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(
                      (accountId) ->
                          new LocalEMRSClient() {
                            @Override
                            public GetJobRunResult getJobRunResult(
                                String applicationId, String jobId) {
                              return new GetJobRunResult()
                                  .withJobRun(new JobRun().withState("Cancelled"));
                            }
                          });

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(
                      flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);

              // 1. Submit REFRESH statement
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.refreshQuery, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);
              // mock index state.
              flintIndexJob.refreshing();

              // 2. Cancel query
              String cancelResponse =
                  asyncQueryExecutorService.cancelQuery(
                      response.getQueryId(), asyncQueryRequestContext);

              assertNotNull(cancelResponse);
              assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

              // assert state is active
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
            });
  }

  /** Cancel REFRESH statement should success */
  @Test
  public void cancelRefreshStatementWithActiveState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(
                      (accountId) ->
                          new LocalEMRSClient() {
                            @Override
                            public GetJobRunResult getJobRunResult(
                                String applicationId, String jobId) {
                              return new GetJobRunResult()
                                  .withJobRun(new JobRun().withState("Cancelled"));
                            }
                          });

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob =
                  new MockFlintSparkJob(
                      flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);

              // 1. Submit REFRESH statement
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(
                          mockDS.refreshQuery, MYS3_DATASOURCE, LangType.SQL, null),
                      asyncQueryRequestContext);
              // mock index state.
              flintIndexJob.active();

              // 2. Cancel query
              IllegalStateException illegalStateException =
                  Assertions.assertThrows(
                      IllegalStateException.class,
                      () ->
                          asyncQueryExecutorService.cancelQuery(
                              response.getQueryId(), asyncQueryRequestContext));
              Assertions.assertEquals(
                  "Transaction failed as flint index is not in a valid state.",
                  illegalStateException.getMessage());

              // assert state is active
              flintIndexJob.assertState(FlintIndexState.ACTIVE);
            });
  }

  @Test
  public void cancelRefreshStatementWithFailureInFetchingIndexMetadata() {
    String indexName = "flint_my_glue_mydb_http_logs_covering_corrupted_index";
    MockFlintIndex mockFlintIndex =
        new MockFlintIndex(client(), indexName, FlintIndexType.COVERING, null);
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(
            (accountId) ->
                new LocalEMRSClient() {
                  @Override
                  public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                    return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
                  }
                });

    mockFlintIndex.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(
            flintIndexStateModelService, indexName + "_latest_id", MYS3_DATASOURCE);

    // 1. Submit REFRESH statement
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(
                "REFRESH INDEX covering_corrupted ON my_glue.mydb.http_logs",
                MYS3_DATASOURCE,
                LangType.SQL,
                null),
            asyncQueryRequestContext);
    // mock index state.
    flintIndexJob.refreshing();

    // 2. Cancel query
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            asyncQueryExecutorService.cancelQuery(response.getQueryId(), asyncQueryRequestContext));
  }
}
