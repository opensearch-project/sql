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
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.executor.execution.QueryPlan;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchQueryManagerTest {

  @Mock private QueryId queryId;

  @Mock private QueryService queryService;

  @Mock private UnresolvedPlan plan;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> listener;

  @Test
  public void submitQuery() {
    NodeClient nodeClient = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(nodeClient.threadPool()).thenReturn(threadPool);

    AtomicBoolean isRun = new AtomicBoolean(false);
    AbstractPlan queryPlan =
        new QueryPlan(queryId, plan, queryService, listener) {
          @Override
          public void execute() {
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
    new OpenSearchQueryManager(nodeClient).submit(queryPlan);

    assertTrue(isRun.get());
  }
}
