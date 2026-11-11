/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;

/** Progress and rows so far of one async query. */
public final class QueryProgressTracker {

  /**
   * Trackers by query task. Kept beside the task rather than on it: plugins such as alerting load
   * the task class without this module, so it must not reference this module's types.
   */
  private static final Map<CancellableTask, QueryProgressTracker> TRACKERS =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final List<SourceProgress> sources = new CopyOnWriteArrayList<>();
  private final PartialRows partialRows = new PartialRows();

  /** Starts tracking the progress of the query run by {@code task}. */
  public static QueryProgressTracker attach(CancellableTask task) {
    QueryProgressTracker tracker = new QueryProgressTracker();
    TRACKERS.put(task, tracker);
    return tracker;
  }

  public static Optional<QueryProgressTracker> current() {
    CancellableTask task = OpenSearchQueryManager.getCancellableTask();
    return task == null ? Optional.empty() : Optional.ofNullable(TRACKERS.get(task));
  }

  public SourceProgress addSource() {
    SourceProgress source = new SourceProgress();
    sources.add(source);
    return source;
  }

  public PartialRows partialRows() {
    return partialRows;
  }

  public double fractionDone() {
    return sources.stream().mapToDouble(SourceProgress::fractionDone).average().orElse(0D);
  }
}
