/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.threadpool.ThreadPool;

@ExtendWith(MockitoExtension.class)
public class SchedulerTest {

  @Mock
  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Test
  public void testSchedule() {
    Mockito.when(nodeClient.threadPool()).thenReturn(threadPool);

    Mockito.doAnswer(
        invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return null;
        })
        .when(threadPool)
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    AtomicBoolean isRun = new AtomicBoolean(false);
    Scheduler.schedule(nodeClient, () -> isRun.set(true));
    Assert.assertTrue(isRun.get());
  }

}
