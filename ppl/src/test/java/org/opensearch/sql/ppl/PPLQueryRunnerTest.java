/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

public class PPLQueryRunnerTest {

  @Test
  public void run_deliversQueryResponseThroughFuture()
      throws ExecutionException, InterruptedException {
    PPLService service = mock(PPLService.class);
    QueryResponse response = new QueryResponse(new Schema(List.of()), List.of(), Cursor.None);
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(response);
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    PPLQueryRunner runner =
        new PPLQueryRunner(
            service,
            new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
            s -> {},
            Clock.systemUTC(),
            () -> {});
    QueryResult result = runner.run().toCompletableFuture().get();
    assertTrue(result instanceof QueryResult.Rows);
    assertEquals(response.getSchema(), ((QueryResult.Rows) result).schema());
    assertTrue(result.tookMillis() >= 0);
  }

  @Test
  public void run_propagatesQueryFailure() {
    PPLService service = mock(PPLService.class);
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onFailure(new IllegalStateException("boom"));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());

    PPLQueryRunner runner =
        new PPLQueryRunner(
            service,
            new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
            s -> {},
            Clock.systemUTC(),
            () -> {});
    ExecutionException e =
        assertThrows(ExecutionException.class, () -> runner.run().toCompletableFuture().get());
    assertEquals("boom", e.getCause().getMessage());
  }

  @Test
  public void run_isSingleUse() {
    PPLQueryRunner runner =
        new PPLQueryRunner(
            mock(PPLService.class),
            new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
            s -> {},
            Clock.systemUTC(),
            () -> {});
    runner.run();
    assertThrows(IllegalStateException.class, runner::run);
  }

  @Test
  public void cancel_stopsExecutionOnceAndCancelsFuture() {
    AtomicInteger stops = new AtomicInteger();
    PPLQueryRunner runner =
        new PPLQueryRunner(
            mock(PPLService.class),
            new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
            s -> {},
            Clock.systemUTC(),
            stops::incrementAndGet);
    var future = runner.run().toCompletableFuture();

    runner.cancel();
    runner.cancel();

    assertEquals(1, stops.get());
    assertThrows(CancellationException.class, future::join);
  }

  @Test
  public void cancel_afterCompletionDoesNotStopExecution() {
    PPLService service = mock(PPLService.class);
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(new QueryResponse(new Schema(List.of()), List.of(), Cursor.None));
              return null;
            })
        .when(service)
        .execute(any(PPLQueryRequest.class), any(), any(), any());
    AtomicInteger stops = new AtomicInteger();
    PPLQueryRunner runner =
        new PPLQueryRunner(
            service,
            new PPLQueryRequest("source=logs", null, "/_plugins/_ppl", "jdbc"),
            s -> {},
            Clock.systemUTC(),
            stops::incrementAndGet);
    QueryResult result = runner.run().toCompletableFuture().join();

    runner.cancel();

    assertEquals(0, stops.get());
    assertTrue(result instanceof QueryResult.Rows);
  }
}
