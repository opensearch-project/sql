/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;

/** Progress of one query: the mean of its index scans, each added at code generation. */
public final class QueryProgressTracker {

  private final List<SourceProgress> sources = new CopyOnWriteArrayList<>();

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

  public double fractionDone() {
    return sources.stream().mapToDouble(SourceProgress::fractionDone).average().orElse(0D);
  }
}
