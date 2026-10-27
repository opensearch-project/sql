/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
  void current_readsTrackerFromProgressTrackingTask() {
    QueryProgressTracker tracker = new QueryProgressTracker();
    CancellableTask task =
        mock(CancellableTask.class, withSettings().extraInterfaces(ProgressTrackingTask.class));
    when(((ProgressTrackingTask) task).progressTracker()).thenReturn(tracker);
    OpenSearchQueryManager.setCancellableTask(task);
    assertSame(tracker, QueryProgressTracker.current().orElseThrow());
  }

  @Test
  void current_isEmptyWithoutProgressTrackingTask() {
    assertTrue(QueryProgressTracker.current().isEmpty());
    OpenSearchQueryManager.setCancellableTask(mock(CancellableTask.class));
    assertTrue(QueryProgressTracker.current().isEmpty());
  }
}
