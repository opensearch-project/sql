/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;

/**
 * Progress of one async query, the mean of its index scans (each added at code generation), and the
 * rows it has produced so far.
 */
public final class QueryProgressTracker {

  private final List<SourceProgress> sources = new CopyOnWriteArrayList<>();
  private final PartialRows partialRows = new PartialRows();

  public static Optional<QueryProgressTracker> current() {
    return OpenSearchQueryManager.getCancellableTask() instanceof ProgressTrackingTask task
        ? Optional.ofNullable(task.progressTracker())
        : Optional.empty();
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
