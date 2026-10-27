/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import javax.annotation.Nullable;

/** A query task that may carry a {@link QueryProgressTracker}; {@code null} for sync queries. */
public interface ProgressTrackingTask {

  @Nullable
  QueryProgressTracker progressTracker();
}
