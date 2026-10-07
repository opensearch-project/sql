/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import java.util.List;
import java.util.Map;
import lombok.Data;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/** AsyncQueryExecutionResponse to store the response form spark job execution. */
@Data
public class AsyncQueryExecutionResponse {
  private final String status;
  private final ExecutionEngine.Schema schema;
  private final List<ExprValue> results;
  private final String error;
  private final String sessionId;

  /**
   * Statement-level {@code explain} result. Populated only when the underlying job produced a
   * {@link org.opensearch.sql.job.QueryResult.Explain}; {@code null} for ordinary row-shaped
   * responses. Transport layers that see a non-null value bypass the standard schema-and-datarows
   * renderer and format this with the sync explain formatter.
   */
  private final ExecutionEngine.ExplainResponse explain;

  /**
   * Structured error payload — the same map {@code SyncErrorReportRenderer} produces for the sync
   * REST path. Populated only on the PPL {@code FAILED} path so an async GET returns the full
   * sync-shape error body. {@code null} on every other path, including every Spark construction
   * site; the formatter keeps the existing {@code "error"}-as-string branch for Spark.
   */
  private final Map<String, Object> errorDetails;

  /** Spark-shaped constructor; keeps callers that never populate structured error details. */
  public AsyncQueryExecutionResponse(
      String status,
      ExecutionEngine.Schema schema,
      List<ExprValue> results,
      String error,
      String sessionId,
      ExecutionEngine.ExplainResponse explain) {
    this(status, schema, results, error, sessionId, explain, null);
  }

  /**
   * Full constructor including the structured error payload. Used by the PPL async failure path.
   */
  public AsyncQueryExecutionResponse(
      String status,
      ExecutionEngine.Schema schema,
      List<ExprValue> results,
      String error,
      String sessionId,
      ExecutionEngine.ExplainResponse explain,
      Map<String, Object> errorDetails) {
    this.status = status;
    this.schema = schema;
    this.results = results;
    this.error = error;
    this.sessionId = sessionId;
    this.explain = explain;
    this.errorDetails = errorDetails;
  }
}
