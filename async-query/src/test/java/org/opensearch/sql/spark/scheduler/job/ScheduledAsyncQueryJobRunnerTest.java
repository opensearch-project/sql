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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.sql.legacy.executor.AsyncRestExecutor;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

public class ScheduledAsyncQueryJobRunnerTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ThreadPool threadPool;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AsyncQueryExecutorService asyncQueryExecutorService;

  @Mock private JobExecutionContext context;

  private ScheduledAsyncQueryJobRunner jobRunner;

  private ScheduledAsyncQueryJobRunner spyJobRunner;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    jobRunner = ScheduledAsyncQueryJobRunner.getJobRunnerInstance();
    jobRunner.loadJobResource(null, null, null, null);
  }

  @Test
  public void testRunJobWithCorrectParameter() {
    spyJobRunner = spy(jobRunner);
    spyJobRunner.loadJobResource(client, clusterService, threadPool, asyncQueryExecutorService);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
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
    verify(threadPool.executor(AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME))
        .submit(captor.capture());

    Runnable runnable = captor.getValue();
    runnable.run();

    verify(spyJobRunner).doRefresh(eq(request));
    verify(asyncQueryExecutorService)
        .createAsyncQuery(eq(createAsyncQueryRequest), any(NullAsyncQueryRequestContext.class));
  }

  @Test
  public void testRunJobWithIncorrectParameter() {
    jobRunner = ScheduledAsyncQueryJobRunner.getJobRunnerInstance();
    jobRunner.loadJobResource(client, clusterService, threadPool, asyncQueryExecutorService);

    ScheduledJobParameter wrongParameter = mock(ScheduledJobParameter.class);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(wrongParameter, context),
            "Expected IllegalStateException but no exception was thrown");

    assertEquals(
        "Job parameter is not instance of ScheduledAsyncQueryJobRequest, type: "
            + wrongParameter.getClass().getCanonicalName(),
        exception.getMessage());
  }

  @Test
  public void testDoRefreshThrowsException() {
    spyJobRunner = spy(jobRunner);
    spyJobRunner.loadJobResource(client, clusterService, threadPool, asyncQueryExecutorService);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId("testJob")
            .lastUpdateTime(Instant.now())
            .lockDurationSeconds(10L)
            .scheduledQuery("REFRESH INDEX testIndex")
            .dataSource("testDataSource")
            .queryLang(LangType.SQL)
            .build();

    doThrow(new RuntimeException("Test exception")).when(spyJobRunner).doRefresh(request);

    Logger logger = LogManager.getLogger(ScheduledAsyncQueryJobRunner.class);
    Appender mockAppender = mock(Appender.class);
    when(mockAppender.getName()).thenReturn("MockAppender");
    when(mockAppender.isStarted()).thenReturn(true);
    when(mockAppender.isStopped()).thenReturn(false);
    ((org.apache.logging.log4j.core.Logger) logger)
        .addAppender((org.apache.logging.log4j.core.Appender) mockAppender);

    spyJobRunner.runJob(request, context);

    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool.executor(AsyncRestExecutor.SQL_WORKER_THREAD_POOL_NAME))
        .submit(captor.capture());

    Runnable runnable = captor.getValue();
    runnable.run();

    verify(spyJobRunner).doRefresh(eq(request));
    verify(mockAppender).append(any(LogEvent.class));
  }

  @Test
  public void testRunJobWithUninitializedServices() {
    ScheduledAsyncQueryJobRequest jobParameter =
        ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder()
            .jobId("testJob")
            .lastUpdateTime(Instant.now())
            .build();

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("ClusterService is not initialized.", exception.getMessage());

    jobRunner.loadJobResource(null, clusterService, null, null);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("ThreadPool is not initialized.", exception.getMessage());

    jobRunner.loadJobResource(null, clusterService, threadPool, null);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("Client is not initialized.", exception.getMessage());

    jobRunner.loadJobResource(client, clusterService, threadPool, null);

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> jobRunner.runJob(jobParameter, context),
            "Expected IllegalStateException but no exception was thrown");
    assertEquals("AsyncQueryExecutorService is not initialized.", exception.getMessage());
  }

  @Test
  public void testGetJobRunnerInstanceMultipleCalls() {
    ScheduledAsyncQueryJobRunner instance1 = ScheduledAsyncQueryJobRunner.getJobRunnerInstance();
    ScheduledAsyncQueryJobRunner instance2 = ScheduledAsyncQueryJobRunner.getJobRunnerInstance();
    ScheduledAsyncQueryJobRunner instance3 = ScheduledAsyncQueryJobRunner.getJobRunnerInstance();

    assertSame(instance1, instance2);
    assertSame(instance2, instance3);
  }
}
