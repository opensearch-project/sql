/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.sql.spark.scheduler.model.OpenSearchRefreshIndexJobRequest;
import org.opensearch.threadpool.ThreadPool;

public class OpenSearchRefreshIndexJobTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ClusterService clusterService;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ThreadPool threadPool;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Client client;

  @Mock private JobExecutionContext context;

  @Mock private LockService lockService;

  private OpenSearchRefreshIndexJob jobRunner;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    jobRunner = OpenSearchRefreshIndexJob.getJobRunnerInstance();
    jobRunner.setClusterService(clusterService);
    jobRunner.setThreadPool(threadPool);
    jobRunner.setClient(client);
    when(context.getLockService()).thenReturn(lockService);
  }

  @Test
  public void testRunJobWithCorrectParameter() {
    OpenSearchRefreshIndexJobRequest jobParameter =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName("testJob")
            .lastUpdateTime(Instant.now())
            .lockDurationSeconds(10L)
            .build();

    jobRunner.runJob(jobParameter, context);

    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    verify(threadPool.generic()).submit(captor.capture());

    Runnable runnable = captor.getValue();
    runnable.run();

    verify(lockService).acquireLock(eq(jobParameter), eq(context), any(ActionListener.class));
  }

  @Test
  public void testRunJobWithIncorrectParameter() {
    ScheduledJobParameter wrongParameter = mock(ScheduledJobParameter.class);

    try {
      jobRunner.runJob(wrongParameter, context);
    } catch (IllegalStateException e) {
      assertEquals(
          "Job parameter is not instance of OpenSearchRefreshIndexJobRequest, type: "
              + wrongParameter.getClass().getCanonicalName(),
          e.getMessage());
    }
  }

  @Test
  public void testRunJobWithUninitializedServices() {
    OpenSearchRefreshIndexJobRequest jobParameter =
        OpenSearchRefreshIndexJobRequest.builder()
            .jobName("testJob")
            .lastUpdateTime(Instant.now())
            .build();

    OpenSearchRefreshIndexJob uninitializedJobRunner =
        OpenSearchRefreshIndexJob.getJobRunnerInstance();

    try {
      uninitializedJobRunner.runJob(jobParameter, context);
    } catch (IllegalStateException e) {
      assertEquals("ClusterService is not initialized.", e.getMessage());
    }

    uninitializedJobRunner.setClusterService(clusterService);

    try {
      uninitializedJobRunner.runJob(jobParameter, context);
    } catch (IllegalStateException e) {
      assertEquals("ThreadPool is not initialized.", e.getMessage());
    }
  }
}
