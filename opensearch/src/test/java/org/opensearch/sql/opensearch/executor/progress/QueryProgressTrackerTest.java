/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;

class QueryProgressTrackerTest {

  @AfterEach
  void clearTask() {
    OpenSearchQueryManager.clearCancellableTask();
  }

  @Test
  void noSources_reportsZero() {
    assertEquals(0D, new QueryProgressTracker().fractionDone());
  }

  @Test
  void fractionDone_isMeanOfSources() {
    QueryProgressTracker tracker = new QueryProgressTracker();
    tracker.addSource().complete();
    tracker.addSource();
    assertEquals(0.5D, tracker.fractionDone());
  }

  @Test
  void current_readsTrackerAttachedToTheTask() {
    CancellableTask task = mock(CancellableTask.class);
    QueryProgressTracker tracker = QueryProgressTracker.attach(task);
    OpenSearchQueryManager.setCancellableTask(task);
    assertSame(tracker, QueryProgressTracker.current().orElseThrow());
  }

  @Test
  void current_isEmptyWithoutAttachedTracker() {
    assertTrue(QueryProgressTracker.current().isEmpty());
    OpenSearchQueryManager.setCancellableTask(mock(CancellableTask.class));
    assertTrue(QueryProgressTracker.current().isEmpty());
  }
}
