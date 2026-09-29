/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.List;
import java.util.Objects;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.executor.pagination.Cursor;

/**
 * Engine-neutral final result of a query.
 *
 * <p>The record composes types already exported by {@code core} ({@link Schema}, {@link ExprValue},
 * {@link Cursor}, {@link Warning}) so that PPL, SQL, and the analytics engine can each construct a
 * {@code QueryResult} without introducing a new dependency on the lifecycle package.
 *
 * @param schema column metadata
 * @param rows result rows in engine order
 * @param cursor continuation cursor for pageable results
 * @param warnings non-fatal notices attached to a successful result
 * @param tookMillis elapsed execution time on the owner node
 */
public record QueryResult(
    Schema schema, List<ExprValue> rows, Cursor cursor, List<Warning> warnings, long tookMillis) {

  /**
   * Validates and normalizes the record's components.
   *
   * <p>Callback engines represent "no continuation" as either {@link Cursor#None} or a plain {@code
   * null} cursor; both are accepted here and stored as {@link Cursor#None}. Likewise, a {@code
   * null} {@code warnings} list is normalized to an empty list. Callers therefore never have to
   * translate before constructing a {@code QueryResult}.
   *
   * @throws NullPointerException if {@code schema} or {@code rows} is {@code null}
   * @throws IllegalArgumentException if {@code tookMillis} is negative
   */
  public QueryResult {
    Objects.requireNonNull(schema, "schema must not be null");
    rows = List.copyOf(Objects.requireNonNull(rows, "rows must not be null"));
    // Callback engines represent "no continuation" as either Cursor.None or a plain null cursor.
    // Normalize here so the record honours the "no null components" invariant without forcing
    // every producer to translate.
    cursor = cursor == null ? Cursor.None : cursor;
    warnings = warnings == null ? List.of() : List.copyOf(warnings);
    if (tookMillis < 0) {
      throw new IllegalArgumentException("tookMillis must not be negative");
    }
  }

  /**
   * Adapts a callback-style {@link QueryResponse} into a {@link QueryResult}. Convenience for
   * engines that already produce {@code QueryResponse} instances.
   */
  public static QueryResult of(QueryResponse response, long tookMillis) {
    Objects.requireNonNull(response, "response must not be null");
    return new QueryResult(
        response.getSchema(),
        response.getResults(),
        response.getCursor(),
        response.getWarnings(),
        tookMillis);
  }
}
