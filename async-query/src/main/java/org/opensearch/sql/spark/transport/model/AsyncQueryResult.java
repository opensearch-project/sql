/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.util.Collection;
import java.util.Map;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.QueryResult;

/** AsyncQueryResult for async query APIs. */
public class AsyncQueryResult extends QueryResult {

  @Getter private final String status;
  @Getter private final String error;

  /**
   * Structured error payload — the same map {@code SyncErrorReportRenderer} produces for the sync
   * REST path. Populated only on the PPL {@code FAILED} path; {@code null} on the Spark path (and
   * on all PPL non-failure paths). When non-{@code null}, the formatter emits it as the {@code
   * "error"} JSON value instead of the {@link #error} string, giving GET on a failed job the same
   * body a sync POST would have returned.
   */
  @Getter private final Map<String, Object> errorDetails;

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error) {
    this(status, schema, exprValues, cursor, error, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error,
      Map<String, Object> errorDetails) {
    super(schema, exprValues, cursor);
    this.status = status;
    this.error = error;
    this.errorDetails = errorDetails;
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error) {
    this(status, schema, exprValues, error, null);
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error,
      Map<String, Object> errorDetails) {
    super(schema, exprValues);
    this.status = status;
    this.error = error;
    this.errorDetails = errorDetails;
  }
}
