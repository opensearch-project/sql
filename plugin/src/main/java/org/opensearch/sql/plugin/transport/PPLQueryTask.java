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

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }

  /**
   * Enable per-thread CPU/memory accounting so the coordinator task's resource usage is exposed to
   * Query Insights. Overridden because {@link CancellableTask} defaults to {@code false}.
   */
  @Override
  public boolean supportsResourceTracking() {
    return true;
  }
}
