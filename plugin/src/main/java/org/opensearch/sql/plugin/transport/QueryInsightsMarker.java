/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

/**
 * Task header linking a child DSL search back to the SQL/PPL query that spawned it, for Query
 * Insights. Value is {@code <source>:<nodeId>:<taskId>}; registered via {@code
 * SQLPlugin.getTaskHeaders()} so OpenSearch copies it onto child search tasks.
 */
public final class QueryInsightsMarker {

  /** Task header name carrying {@code <source>:<nodeId>:<taskId>} of the originating query. */
  public static final String PARENT_HEADER = "X-Query-Insights-Parent";

  private QueryInsightsMarker() {}

  /** Build the {@code <source>:<nodeId>:<taskId>} header value for a coordinator query. */
  public static String value(String source, String nodeId, long taskId) {
    return source + ":" + nodeId + ":" + taskId;
  }
}
