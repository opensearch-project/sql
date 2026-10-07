/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.sql.common.error.QueryProcessingStage;
import org.opensearch.sql.common.error.StageErrorHandler;
import org.opensearch.sql.exception.CalciteUnsupportedException;

class QueryServiceCancellationTest {

  @Test
  void detectsTaskCancellationBehindExecutionWrappers() {
    assertTrue(QueryService.isCancellation(executionFailure(new TaskCancelledException("x"))));
  }

  @Test
  void detectsFutureCancellationBehindExecutionWrappers() {
    assertTrue(QueryService.isCancellation(executionFailure(new CancellationException("x"))));
  }

  @Test
  void ignoresOrdinaryAndUnsupportedFailures() {
    assertFalse(QueryService.isCancellation(null));
    assertFalse(QueryService.isCancellation(executionFailure(new IllegalStateException("x"))));
    assertFalse(QueryService.isCancellation(new CalciteUnsupportedException("unsupported")));
  }

  @Test
  void terminatesOnMultiNodeCauseCycle() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    first.initCause(second);
    second.initCause(first);

    assertTimeoutPreemptively(
        Duration.ofSeconds(1), () -> assertFalse(QueryService.isCancellation(first)));
  }

  @Test
  void findsCancellationInsideCauseCycle() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    TaskCancelledException cancelled = new TaskCancelledException("cancelled");
    first.initCause(second);
    second.initCause(cancelled);
    cancelled.initCause(first);

    assertTimeoutPreemptively(
        Duration.ofSeconds(1), () -> assertTrue(QueryService.isCancellation(first)));
  }

  private static Exception executionFailure(Exception cause) {
    return StageErrorHandler.wrapWithStage(
        QueryProcessingStage.EXECUTING,
        new RuntimeException(new SQLException("Error while executing SQL", cause)),
        "while running the query");
  }
}
