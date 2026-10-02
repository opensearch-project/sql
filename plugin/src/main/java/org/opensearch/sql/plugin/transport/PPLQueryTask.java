/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.List;
import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask {

  // The fields below are captured on the request thread and read later by the report listener
  // (which runs on a different thread), for the Query Insights record.

  /** Security {@code _opendistro_security_user_info} string; null when unsecured. */
  private volatile String queryInsightsUserInfo;

  /** Source index name(s) resolved from the AST; empty when unresolved. */
  private volatile List<String> queryInsightsIndices = List.of();

  /** Anonymized query text ({@code PPLQueryDataAnonymizer}); null when anonymization didn't run. */
  private volatile String queryInsightsAnonymizedQuery;

  /** Whether the query failed; recorded so a failed PPL query is flagged in Top N. */
  private volatile boolean queryInsightsFailed = false;

  /**
   * Whether per-thread CPU/memory accounting is enabled for this task. Off by default so a normal
   * PPL query behaves like any {@link CancellableTask}; turned on only when Query Insights
   * recording and core's task resource tracking are both enabled.
   */
  private volatile boolean resourceTrackingEnabled = false;

  public PPLQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  public void setQueryInsightsUserInfo(String userInfo) {
    this.queryInsightsUserInfo = userInfo;
  }

  public String getQueryInsightsUserInfo() {
    return queryInsightsUserInfo;
  }

  public void setQueryInsightsIndices(List<String> indices) {
    this.queryInsightsIndices = indices == null ? List.of() : indices;
  }

  public List<String> getQueryInsightsIndices() {
    return queryInsightsIndices;
  }

  public void setQueryInsightsAnonymizedQuery(String anonymizedQuery) {
    this.queryInsightsAnonymizedQuery = anonymizedQuery;
  }

  public String getQueryInsightsAnonymizedQuery() {
    return queryInsightsAnonymizedQuery;
  }

  public void setQueryInsightsFailed(boolean failed) {
    this.queryInsightsFailed = failed;
  }

  public boolean isQueryInsightsFailed() {
    return queryInsightsFailed;
  }

  public void setResourceTrackingEnabled(boolean enabled) {
    this.resourceTrackingEnabled = enabled;
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }

  /**
   * Per-thread CPU/memory accounting for the coordinator task, enabled only when Query Insights
   * recording and core's {@code task_resource_tracking.enabled} are both on (see {@code
   * TransportPPLQueryAction}). {@link CancellableTask} defaults to {@code false}.
   */
  @Override
  public boolean supportsResourceTracking() {
    return resourceTrackingEnabled;
  }
}
