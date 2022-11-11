/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.execution.AbstractPlan;

@ExtendWith(MockitoExtension.class)
class DefaultQueryManagerTest {

  @Mock
  private QueryId queryId;

  @Mock
  private AbstractPlan plan;

  private DefaultQueryManager queryManager;

  private ExecutorService executorService;

  @AfterEach
  void clean() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void submitQuery() {
    when(plan.getQueryId()).thenReturn(queryId);

    queryManager = DefaultQueryManager.defaultQueryManager();;
    QueryId actualQueryId = queryManager.submit(plan);

    assertEquals(queryId, actualQueryId);
    verify(plan, times(1)).execute();
  }

  @Test
  public void cancel() {
    queryManager = DefaultQueryManager.defaultQueryManager();;
    QueryId id = queryManager.submit(new AbstractPlan(queryId) {
      @Override
      public void execute() {
        // do nothing
      }

      @Override
      public void explain(ResponseListener<ExecutionEngine.ExplainResponse> listener) {
        // do nothing
      }
    });
    assertTrue(queryManager.cancel(id));
    assertFalse(queryManager.cancel(id));
  }

  @Test
  public void executorServiceShutdownProperly() throws InterruptedException {
    executorService = Executors.newSingleThreadExecutor();
    queryManager = new DefaultQueryManager(executorService);
    queryManager.submit(plan);
    queryManager.awaitTermination(1, TimeUnit.SECONDS);

    assertTrue(executorService.isShutdown());
    assertTrue(executorService.isTerminated());
  }
}
