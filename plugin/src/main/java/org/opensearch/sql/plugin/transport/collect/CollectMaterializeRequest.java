/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.collect;

import java.io.IOException;
import java.util.Map;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;

/**
 * Request to run a terminal {@code collect} query's write in the background. Carries the original
 * PPL text (re-analyzed and executed by the transport action) and the destination index name (used
 * only for the task description). {@link #getShouldStoreResult()} returns true so the core task
 * machinery persists the outcome to {@code .tasks}; the task is a {@link CancellableTask} so {@code
 * POST _tasks/<id>/_cancel} works.
 */
public class CollectMaterializeRequest extends ActionRequest {

  private final String pplQuery;
  private final String indexName;

  public CollectMaterializeRequest(String pplQuery, String indexName) {
    this.pplQuery = pplQuery;
    this.indexName = indexName;
  }

  public CollectMaterializeRequest(StreamInput in) throws IOException {
    super(in);
    this.pplQuery = in.readString();
    this.indexName = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(pplQuery);
    out.writeString(indexName);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getPplQuery() {
    return pplQuery;
  }

  public String getIndexName() {
    return indexName;
  }

  @Override
  public boolean getShouldStoreResult() {
    return true;
  }

  @Override
  public Task createTask(
      long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
    return new CancellableTask(
        id, type, action, "collect materialize to [" + indexName + "]", parentTaskId, headers) {
      @Override
      public boolean shouldCancelChildrenOnCancellation() {
        return true;
      }
    };
  }
}
