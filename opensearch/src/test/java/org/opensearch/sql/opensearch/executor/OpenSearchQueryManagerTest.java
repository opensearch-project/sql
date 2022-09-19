/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.execution.QueryExecution;
import org.opensearch.threadpool.ThreadPool;

@ExtendWith(MockitoExtension.class)
class OpenSearchQueryManagerTest {

  @Mock
  private ResponseListener<?> listener;

  @Mock
  private QueryId queryId;

  @Test
  public void submitQuery() {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);

    AtomicBoolean isRun = new AtomicBoolean(false);
    AtomicBoolean isListenerRegistered = new AtomicBoolean(false);
    QueryExecution queryExecution = new QueryExecution(queryId) {
      @Override
      public void registerListener(ResponseListener<?> listener) {
        isListenerRegistered.set(true);
      }

      @Override
      public void start() {
        isRun.set(true);
      }
    };

    doAnswer(
        invocation -> {
          Runnable task = invocation.getArgument(0);
          task.run();
          return null;
        })
        .when(threadPool)
        .schedule(any(), any(), any());
    new OpenSearchQueryManager(nodeClient).submitQuery(queryExecution, listener);

    assertTrue(isRun.get());
    assertTrue(isListenerRegistered.get());
  }
}
