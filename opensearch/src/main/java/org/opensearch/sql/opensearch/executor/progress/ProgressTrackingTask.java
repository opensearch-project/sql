/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import javax.annotation.Nullable;

/** A query task that may carry a {@link QueryProgressTracker}. */
public interface ProgressTrackingTask {

  /** Returns the tracker, or {@code null} when this query does not report progress. */
  @Nullable
  QueryProgressTracker progressTracker();
}
