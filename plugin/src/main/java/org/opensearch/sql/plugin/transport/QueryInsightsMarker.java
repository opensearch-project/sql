/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

/**
 * Constants and helpers for the task header that links a child DSL search back to the SQL/PPL query
 * that spawned it, for Query Insights.
 *
 * <p>The value is {@code <source>:<nodeId>:<taskId>} (e.g. {@code PPL:node-1:42}). Registered via
 * {@code SQLPlugin.getTaskHeaders()} so OpenSearch copies it onto child search tasks, where Query
 * Insights reads it to classify each child and link it to its parent. Source-neutral so the same
 * mechanism serves PPL now and SQL later.
 */
public final class QueryInsightsMarker {

  /** Task header name carrying {@code <source>:<nodeId>:<taskId>} of the originating query. */
  public static final String PARENT_HEADER = "X-Query-Insights-Parent";

  private QueryInsightsMarker() {}

  /**
   * Build the header value for a coordinator query.
   *
   * @param source query source label (e.g. {@code "PPL"})
   * @param nodeId coordinator node id
   * @param taskId coordinator task id
   * @return the {@code <source>:<nodeId>:<taskId>} header value
   */
  public static String value(String source, String nodeId, long taskId) {
    return source + ":" + nodeId + ":" + taskId;
  }
}
