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

public class IndexQuerySpecDropTest extends IndexQuerySpecBase {
    /**
     * Happy case. expectation is
     *
     * <p>(1) Drop Index response is SUCCESS (2) change index state to: DELETED
     */
    @Test
    public void dropAndFetchAndCancel() {
        ImmutableList.of(SKIPPING, COVERING, MV, SPECIAL_CHARACTERS)
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
                            // Mock index state
                            MockFlintSparkJob flintIndexJob =
                                    new MockFlintSparkJob(
                                            flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);
                            flintIndexJob.refreshing();

                            // 1.drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            assertNotNull(response.getQueryId());
                            assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

                            // assert state is DELETED
                            flintIndexJob.assertState(FlintIndexState.DELETED);

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
     * Cancel EMR-S job, but not job running. expectation is
     *
     * <p>(1) Drop Index response is SUCCESS (2) change index state to: DELETED
     */
    @Test
    public void dropIndexNoJobRunning() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            // Mock EMR-S job is not running
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
                            // Mock index state in refresh state.
                            MockFlintSparkJob flintIndexJob =
                                    new MockFlintSparkJob(
                                            flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);
                            flintIndexJob.refreshing();

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

                            flintIndexJob.assertState(FlintIndexState.DELETED);
                        });
    }

    /**
     * Cancel EMR-S job call timeout, expectation is
     *
     * <p>(1) Drop Index response is failed, (2) change index state to: CANCELLING
     */
    @Test
    public void dropIndexCancelJobTimeout() {
        ImmutableList.of(SKIPPING, COVERING, MV)
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
                            // Mock index state
                            MockFlintSparkJob flintIndexJob =
                                    new MockFlintSparkJob(
                                            flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);
                            flintIndexJob.refreshing();

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
                            flintIndexJob.assertState(FlintIndexState.REFRESHING);
                        });
    }

    /**
     * Cancel EMR-S job, but not job running. expectation is
     *
     * <p>(1) Drop Index response is SUCCESS (2) change index state to: DELETED
     */
    @Test
    public void dropIndexSpecialCharacter() {
        FlintDatasetMock mockDS = SPECIAL_CHARACTERS;
        // Mock EMR-S job is not running
        LocalEMRSClient emrsClient =
                new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(
                            String applicationId, String jobId, boolean allowExceptionPropagation) {
                        throw new IllegalArgumentException("Job run is not in a cancellable state");
                    }
                };
        EMRServerlessClientFactory emrServerlessClientFactory = (accountId) -> emrsClient;
        AsyncQueryExecutorService asyncQueryExecutorService =
                createAsyncQueryExecutorService(emrServerlessClientFactory);

        // Mock flint index
        mockDS.createIndex();
        // Mock index state in refresh state.
        MockFlintSparkJob flintIndexJob =
                new MockFlintSparkJob(flintIndexStateModelService, mockDS.latestId, MYGLUE_DATASOURCE);
        flintIndexJob.refreshing();

        // 1.drop index
        CreateAsyncQueryResponse response =
                asyncQueryExecutorService.createAsyncQuery(
                        new CreateAsyncQueryRequest(mockDS.query, MYGLUE_DATASOURCE, LangType.SQL, null),
                        asyncQueryRequestContext);

        // 2.fetch result.
        AsyncQueryExecutionResponse asyncQueryResults =
                asyncQueryExecutorService.getAsyncQueryResults(
                        response.getQueryId(), asyncQueryRequestContext);
        assertEquals("FAILED", asyncQueryResults.getStatus());
        assertEquals("Internal Server Error.", asyncQueryResults.getError());

        flintIndexJob.assertState(FlintIndexState.REFRESHING);
    }
}
