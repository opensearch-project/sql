/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;

/**
 * Combines the progress of every index scan in one query. Each physical scan registers once, at
 * code generation, so the set of sources is known before the first search is sent. The query's
 * progress is the mean of its sources' progress.
 */
public final class QueryProgressTracker {

  private final Map<Object, SourceProgress> sources = new IdentityHashMap<>();

  /** Returns the tracker of the query running on this thread, if that query reports progress. */
  public static Optional<QueryProgressTracker> current() {
    return OpenSearchQueryManager.getCancellableTask() instanceof ProgressTrackingTask task
        ? Optional.ofNullable(task.progressTracker())
        : Optional.empty();
  }

  /** Returns the progress of {@code scan}, registering it on first use. */
  public synchronized SourceProgress register(Object scan) {
    return sources.computeIfAbsent(scan, key -> new SourceProgress());
  }

  /** Returns the mean progress of all registered sources, or {@code 0.0} if there are none. */
  public double fractionDone() {
    List<SourceProgress> snapshot;
    synchronized (this) {
      snapshot = new ArrayList<>(sources.values());
    }
    if (snapshot.isEmpty()) {
      return 0D;
    }
    double sum = 0D;
    for (SourceProgress source : snapshot) {
      sum += source.fractionDone();
    }
    return Math.min(1D, sum / snapshot.size());
  }
}
