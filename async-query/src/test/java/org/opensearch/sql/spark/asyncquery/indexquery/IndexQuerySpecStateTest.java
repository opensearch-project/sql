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
import org.jetbrains.annotations.NotNull;
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

public class IndexQuerySpecStateTest extends IndexQuerySpecBase {
    /**
     * No Job running, expectation is
     *
     * <p>(1) not call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void edgeCaseNoIndexStateDoc() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            EMRServerlessClientFactory emrServerlessClientFactory = getEmrServerlessClientFactory();
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
                            assertTrue(asyncQueryResults.getError().contains("no state found"));
                        });
    }

    /**
     * Drop Index operation is retryable, expectation is
     *
     * <p>(1) call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void dropIndexWithIndexInRefreshingState() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            LocalEMRSClient emrsClient =
                                    new LocalEMRSClient() {
                                        @Override
                                        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                                            super.getJobRunResult(applicationId, jobId);
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

                            // 1. drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            // 2. fetch result
                            assertEquals(
                                    "SUCCESS",
                                    asyncQueryExecutorService
                                            .getAsyncQueryResults(response.getQueryId(), asyncQueryRequestContext)
                                            .getStatus());

                            flintIndexJob.assertState(FlintIndexState.DELETED);
                            emrsClient.startJobRunCalled(0);
                            emrsClient.cancelJobRunCalled(1);
                            emrsClient.getJobRunResultCalled(1);
                        });
    }

    /**
     * Index state is stable, Drop Index operation is retryable, expectation is
     *
     * <p>(1) call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void dropIndexWithIndexInActiveState() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            LocalEMRSClient emrsClient =
                                    new LocalEMRSClient() {
                                        @Override
                                        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                                            super.getJobRunResult(applicationId, jobId);
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
                            flintIndexJob.active();

                            // 1. drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            // 2. fetch result
                            AsyncQueryExecutionResponse asyncQueryExecutionResponse =
                                    asyncQueryExecutorService.getAsyncQueryResults(
                                            response.getQueryId(), asyncQueryRequestContext);
                            assertEquals("SUCCESS", asyncQueryExecutionResponse.getStatus());
                            flintIndexJob.assertState(FlintIndexState.DELETED);
                            emrsClient.startJobRunCalled(0);
                            emrsClient.cancelJobRunCalled(1);
                            emrsClient.getJobRunResultCalled(1);
                        });
    }

    /**
     * Index state is stable, expectation is
     *
     * <p>(1) call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void dropIndexWithIndexInCreatingState() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            LocalEMRSClient emrsClient =
                                    new LocalEMRSClient() {
                                        @Override
                                        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                                            super.getJobRunResult(applicationId, jobId);
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
                            flintIndexJob.creating();

                            // 1. drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            // 2. fetch result
                            assertEquals(
                                    "SUCCESS",
                                    asyncQueryExecutorService
                                            .getAsyncQueryResults(response.getQueryId(), asyncQueryRequestContext)
                                            .getStatus());

                            flintIndexJob.assertState(FlintIndexState.DELETED);
                        });
    }

    /**
     * Index state is stable, Drop Index operation is retryable, expectation is
     *
     * <p>(1) call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void dropIndexWithIndexInEmptyState() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
                            LocalEMRSClient emrsClient =
                                    new LocalEMRSClient() {
                                        @Override
                                        public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                                            super.getJobRunResult(applicationId, jobId);
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

                            // 1. drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            // 2. fetch result
                            assertEquals(
                                    "SUCCESS",
                                    asyncQueryExecutorService
                                            .getAsyncQueryResults(response.getQueryId(), asyncQueryRequestContext)
                                            .getStatus());

                            flintIndexJob.assertState(FlintIndexState.DELETED);
                        });
    }

    /**
     * Couldn't acquire lock as the index is in transitioning state. Will result in error.
     *
     * <p>(1) not call EMR-S (2) change index state to: DELETED
     */
    @Test
    public void dropIndexWithIndexInDeletedState() {
        ImmutableList.of(SKIPPING, COVERING, MV)
                .forEach(
                        mockDS -> {
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

                            // Mock flint index
                            mockDS.createIndex();
                            // Mock index state
                            MockFlintSparkJob flintIndexJob =
                                    new MockFlintSparkJob(
                                            flintIndexStateModelService, mockDS.latestId, MYS3_DATASOURCE);
                            flintIndexJob.deleting();

                            // 1. drop index
                            CreateAsyncQueryResponse response =
                                    asyncQueryExecutorService.createAsyncQuery(
                                            new CreateAsyncQueryRequest(
                                                    mockDS.query, MYS3_DATASOURCE, LangType.SQL, null),
                                            asyncQueryRequestContext);

                            AsyncQueryExecutionResponse asyncQueryExecutionResponse =
                                    asyncQueryExecutorService.getAsyncQueryResults(
                                            response.getQueryId(), asyncQueryRequestContext);
                            // 2. fetch result
                            assertEquals("FAILED", asyncQueryExecutionResponse.getStatus());
                            assertEquals(
                                    "Transaction failed as flint index is not in a valid state.",
                                    asyncQueryExecutionResponse.getError());
                            flintIndexJob.assertState(FlintIndexState.DELETING);
                        });
    }

    @NotNull
    private static EMRServerlessClientFactory getEmrServerlessClientFactory() {
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
        return emrServerlessClientFactory;
    }
}
