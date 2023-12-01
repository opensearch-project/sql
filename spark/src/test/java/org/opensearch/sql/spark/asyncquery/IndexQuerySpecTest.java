/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.leasemanager.ConcurrencyLimitExceededException;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexQuerySpecTest extends AsyncQueryExecutorServiceSpec {

  public final FlintDatasetMock LEGACY_SKIPPING =
      new FlintDatasetMock(
              "DROP SKIPPING INDEX ON mys3.default.http_logs",
              FlintIndexType.SKIPPING,
              "flint_mys3_default_http_logs_skipping_index")
          .isLegacy(true);
  public final FlintDatasetMock LEGACY_COVERING =
      new FlintDatasetMock(
              "DROP INDEX covering ON mys3.default.http_logs",
              FlintIndexType.COVERING,
              "flint_mys3_default_http_logs_covering_index")
          .isLegacy(true);
  public final FlintDatasetMock LEGACY_MV =
      new FlintDatasetMock(
              "DROP MATERIALIZED VIEW mv", FlintIndexType.MATERIALIZED_VIEW, "flint_mv")
          .isLegacy(true);

  public final FlintDatasetMock SKIPPING =
      new FlintDatasetMock(
              "DROP SKIPPING INDEX ON mys3.default.http_logs",
              FlintIndexType.SKIPPING,
              "flint_mys3_default_http_logs_skipping_index")
          .latestId("skippingindexid");
  public final FlintDatasetMock COVERING =
      new FlintDatasetMock(
              "DROP INDEX covering ON mys3.default.http_logs",
              FlintIndexType.COVERING,
              "flint_mys3_default_http_logs_covering_index")
          .latestId("coveringid");
  public final FlintDatasetMock MV =
      new FlintDatasetMock(
              "DROP MATERIALIZED VIEW mv", FlintIndexType.MATERIALIZED_VIEW, "flint_mv")
          .latestId("mvid");

  /**
   * Happy case. expectation is
   *
   * <p>(1) Drop Index response is SUCCESS
   */
  @Test
  public void legacyBasicDropAndFetchAndCancel() {
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
                    }
                  };

              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              assertNotNull(response.getQueryId());
              assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

              // 2.fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
              assertEquals("SUCCESS", asyncQueryResults.getStatus());
              assertNull(asyncQueryResults.getError());
              emrsClient.cancelJobRunCalled(1);

              // 3.cancel
              IllegalArgumentException exception =
                  assertThrows(
                      IllegalArgumentException.class,
                      () -> asyncQueryExecutorService.cancelQuery(response.getQueryId()));
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
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING, LEGACY_MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      throw new IllegalArgumentException("Job run is not in a cancellable state");
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2.fetch result.
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
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
    ImmutableList.of(LEGACY_SKIPPING, LEGACY_COVERING, LEGACY_MV)
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
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
              assertEquals("FAILED", asyncQueryResults.getStatus());
              assertEquals("cancel job timeout", asyncQueryResults.getError());
            });
  }

  /**
   * Happy case. expectation is
   *
   * <p>(1) Drop Index response is SUCCESS (2) change index state to: DELETED
   */
  @Test
  public void dropAndFetchAndCancel() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
                    }
                  };

              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.refreshing();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              assertNotNull(response.getQueryId());
              assertTrue(clusterService.state().routingTable().hasIndex(mockDS.indexName));

              // assert state is DELETED
              flintIndexJob.assertState(FlintIndexState.DELETED);

              // 2.fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
              assertEquals("SUCCESS", asyncQueryResults.getStatus());
              assertNull(asyncQueryResults.getError());
              emrsClient.cancelJobRunCalled(1);

              // 3.cancel
              IllegalArgumentException exception =
                  assertThrows(
                      IllegalArgumentException.class,
                      () -> asyncQueryExecutorService.cancelQuery(response.getQueryId()));
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
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      throw new IllegalArgumentException("Job run is not in a cancellable state");
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state in refresh state.
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.refreshing();

              // 1.drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2.fetch result.
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
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
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.refreshing();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
              assertEquals("FAILED", asyncQueryResults.getStatus());
              assertEquals("cancel job timeout", asyncQueryResults.getError());

              flintIndexJob.assertState(FlintIndexState.CANCELLING);
            });
  }

  /**
   * Drop Index operation is retryable, expectation is
   *
   * <p>(1) call EMR-S (2) change index state to: DELETED
   */
  @Test
  public void dropIndexWithIndexInCancellingState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      return new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled"));
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.cancelling();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

  /**
   * No Job running, expectation is
   *
   * <p>(1) not call EMR-S (2) change index state to: DELETED
   */
  @Test
  public void dropIndexWithIndexInActiveState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.active();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

  @Test
  public void dropIndexWithIndexInDeletingState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.deleted();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

  @Test
  public void dropIndexWithIndexInDeletedState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.deleting();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

  /**
   * No Job running, expectation is
   *
   * <p>(1) not call EMR-S (2) change index state to: DELETED
   */
  @Test
  public void dropIndexWithIndexInEmptyState() {
    ImmutableList.of(SKIPPING, COVERING, MV)
        .forEach(
            mockDS -> {
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();
              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

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
              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrsClient);

              // Mock flint index
              mockDS.createIndex();

              // 1. drop index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              AsyncQueryExecutionResponse asyncQueryResults =
                  asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
              assertEquals("FAILED", asyncQueryResults.getStatus());
              assertTrue(asyncQueryResults.getError().contains("no state found"));
            });
  }

  @Test
  public void concurrentRefreshJobLimitNotApplied() {
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(new LocalEMRSClient());

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(COVERING.latestId);
    flintIndexJob.refreshing();

    // query with auto refresh
    String query =
        "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, "
            + "l_quantity) WITH (auto_refresh = true)";
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null));
    assertNull(response.getSessionId());
  }

  @Test
  public void concurrentRefreshJobLimitAppliedToDDLWithAuthRefresh() {
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(new LocalEMRSClient());

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(COVERING.latestId);
    flintIndexJob.refreshing();

    // query with auto_refresh = true.
    String query =
        "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, "
            + "l_quantity) WITH (auto_refresh = true)";
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null)));
    assertEquals("domain concurrent refresh job can not exceed 1", exception.getMessage());
  }

  @Test
  public void concurrentRefreshJobLimitAppliedToRefresh() {
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(new LocalEMRSClient());

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(COVERING.latestId);
    flintIndexJob.refreshing();

    // query with auto_refresh = true.
    String query = "REFRESH INDEX covering ON mys3.default.http_logs";
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null)));
    assertEquals("domain concurrent refresh job can not exceed 1", exception.getMessage());
  }

  @Test
  public void concurrentRefreshJobLimitNotAppliedToDDL() {
    String query = "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, l_quantity)";

    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(new LocalEMRSClient());

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(COVERING.latestId);
    flintIndexJob.refreshing();

    CreateAsyncQueryResponse asyncQueryResponse =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(query, DATASOURCE, LangType.SQL, null));
    assertNotNull(asyncQueryResponse.getSessionId());
  }
}
