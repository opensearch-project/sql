/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;
import org.opensearch.threadpool.ThreadPool;

public class ScheduledAsyncQueryJobTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ThreadPool threadPool;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AsyncQueryExecutorService asyncQueryExecutorService;

  @Mock private JobExecutionContext context;

  private ScheduledAsyncQueryJob jobRunner;

  private ScheduledAsyncQueryJob spyJobRunner;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    jobRunner = ScheduledAsyncQueryJob.getJobRunnerInstance();
    jobRunner.setClient(null);
    jobRunner.setClusterService(null);
    jobRunner.setThreadPool(null);
    jobRunner.setAsyncQueryExecutorService(null);
  }

  @Test
  public void testRunJobWithCorrectParameter() {
    spyJobRunner = spy(jobRunner);
    spyJobRunner.setClusterService(clusterService);
    spyJobRunner.setThreadPool(threadPool);
    spyJobRunner.setClient(client);
    spyJobRunner.setAsyncQueryExecutorService(asyncQueryExecutorService);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.builder()
            .jobId("testJob")
            .lastUpdateTime(Instant.now())
            .lockDurationSeconds(10L)
            .scheduledQuery("REFRESH INDEX testIndex")
            .dataSource("testDataSource")
            .queryLang(LangType.SQL)
            .build();

    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest(
            request.getScheduledQuery(), request.getDataSource(), request.getQueryLang());
    spyJobRunner.runJob(request, context);

    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool.generic()).submit(captor.capture());

    Runnable runnable = captor.getValue();
    runnable.run();

    verify(spyJobRunner).doRefresh(eq(request));
    verify(asyncQueryExecutorService)
        .createAsyncQuery(eq(createAsyncQueryRequest), any(NullAsyncQueryRequestContext.class));
  }

  @Test
  public void testRunJobWithIncorrectParameter() {
    jobRunner = ScheduledAsyncQueryJob.getJobRunnerInstance();
    jobRunner.setClusterService(clusterService);
    jobRunner.setThreadPool(threadPool);
    jobRunner.setClient(client);
    jobRunner.setAsyncQueryExecutorService(asyncQueryExecutorService);

    ScheduledJobParameter wrongParameter = mock(ScheduledJobParameter.class);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(wrongParameter, context),
            "Expected IllegalStateException but no exception was thrown");

    assertEquals(
        "Job parameter is not instance of OpenSearchRefreshIndexJobRequest, type: "
            + wrongParameter.getClass().getCanonicalName(),
        exception.getMessage());
  }

  @Test
  public void testRunJobWithUninitializedServices() {
    ScheduledAsyncQueryJobRequest jobParameter =
        ScheduledAsyncQueryJobRequest.builder()
            .jobId("testJob")
            .lastUpdateTime(Instant.now())
            .build();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("ClusterService is not initialized.", exception.getMessage());

    jobRunner.setClusterService(clusterService);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("ThreadPool is not initialized.", exception.getMessage());

    jobRunner.setThreadPool(threadPool);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("Client is not initialized.", exception.getMessage());

    jobRunner.setClient(client);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("AsyncQueryExecutorService is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetJobRunnerInstanceMultipleCalls() {
    ScheduledAsyncQueryJob instance1 = ScheduledAsyncQueryJob.getJobRunnerInstance();
    ScheduledAsyncQueryJob instance2 = ScheduledAsyncQueryJob.getJobRunnerInstance();
    ScheduledAsyncQueryJob instance3 = ScheduledAsyncQueryJob.getJobRunnerInstance();

    assertSame(instance1, instance2);
    assertSame(instance2, instance3);
  }
}
