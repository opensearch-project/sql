/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.progress.ProgressTrackingTask;
import org.opensearch.sql.opensearch.executor.progress.QueryProgressTracker;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask implements ProgressTrackingTask {

  private volatile QueryProgressTracker progressTracker;

  public PPLQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }

  /** Called only for async submissions. */
  public QueryProgressTracker trackProgress() {
    progressTracker = new QueryProgressTracker();
    return progressTracker;
  }

  @Override
  public QueryProgressTracker progressTracker() {
    return progressTracker;
  }
}
