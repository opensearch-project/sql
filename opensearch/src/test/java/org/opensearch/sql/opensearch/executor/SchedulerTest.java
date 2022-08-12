/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.threadpool.ThreadPool;

@ExtendWith(MockitoExtension.class)
class SchedulerTest {
  @Test
  public void schedule() {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);

    doAnswer(
        invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return null;
        })
        .when(threadPool)
        .schedule(any(), any(), any());
    AtomicBoolean isRun = new AtomicBoolean(false);
    Scheduler.schedule(nodeClient, () -> isRun.set(true));
    assertTrue(isRun.get());
  }
}
