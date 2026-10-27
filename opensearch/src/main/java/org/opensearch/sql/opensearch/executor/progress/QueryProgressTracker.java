/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;

/**
 * Combines the progress of every index scan in one query. Each physical scan adds its source once,
 * at code generation, so the set of sources is known before the first search is sent. The query's
 * progress is the mean of its sources' progress. Sources hold no plan references, so a retained job
 * does not keep its plan alive.
 */
public final class QueryProgressTracker {

  private final List<SourceProgress> sources = new ArrayList<>();

  /** Returns the tracker of the query running on this thread, if that query reports progress. */
  public static Optional<QueryProgressTracker> current() {
    return OpenSearchQueryManager.getCancellableTask() instanceof ProgressTrackingTask task
        ? Optional.ofNullable(task.progressTracker())
        : Optional.empty();
  }

  /** Adds and returns the progress of one more index scan. */
  public synchronized SourceProgress addSource() {
    SourceProgress source = new SourceProgress();
    sources.add(source);
    return source;
  }

  /** Returns the mean progress of all registered sources, or {@code 0.0} if there are none. */
  public double fractionDone() {
    List<SourceProgress> snapshot;
    synchronized (this) {
      snapshot = new ArrayList<>(sources);
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
