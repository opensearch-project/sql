/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.utils;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.node.NodeClient;
import org.opensearch.threadpool.ThreadPool;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {

  @Mock
  private NodeClient nodeClient;

  @Mock
  private ThreadPool threadPool;

  @Test
  public void testSchedule() {
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