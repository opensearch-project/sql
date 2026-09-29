/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.List;
import java.util.Objects;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.executor.pagination.Cursor;

/**
 * Engine-neutral final result of a query.
 *
 * <p>Sealed to two variants:
 *
 * <ul>
 *   <li>{@link Rows} — the ordinary case: schema + rows + cursor + warnings.
 *   <li>{@link Explain} — statement-level {@code explain} output: the {@link ExplainResponse}
 *       produced when the query text itself is an explain command (e.g. {@code explain source=x |
 *       fields y}). Async submit can carry this shape end-to-end.
 * </ul>
 *
 * <p>Both variants share {@link #tookMillis()} so callers that only need timing don't have to
 * switch on the variant.
 */
public sealed interface QueryResult permits QueryResult.Rows, QueryResult.Explain {

  /** Elapsed execution time on the owner node. */
  long tookMillis();

  /**
   * Row-shaped result — the ordinary query response.
   *
   * @param schema column metadata
   * @param rows result rows in engine order
   * @param cursor continuation cursor for pageable results
   * @param warnings non-fatal notices attached to a successful result
   * @param tookMillis elapsed execution time on the owner node
   */
  record Rows(
      Schema schema, List<ExprValue> rows, Cursor cursor, List<Warning> warnings, long tookMillis)
      implements QueryResult {

    /**
     * Validates and normalizes the record's components.
     *
     * <p>Callback engines represent "no continuation" as either {@link Cursor#None} or a plain
     * {@code null} cursor; both are accepted here and stored as {@link Cursor#None}. A {@code null}
     * {@code warnings} list is normalized to an empty list.
     */
    public Rows {
      Objects.requireNonNull(schema, "schema must not be null");
      rows = List.copyOf(Objects.requireNonNull(rows, "rows must not be null"));
      cursor = cursor == null ? Cursor.None : cursor;
      warnings = warnings == null ? List.of() : List.copyOf(warnings);
      if (tookMillis < 0) {
        throw new IllegalArgumentException("tookMillis must not be negative");
      }
    }
  }

  /**
   * Explain-shaped result — produced when the query text is a statement-level explain (e.g. {@code
   * explain source=x | fields y}).
   *
   * @param response engine-produced explain payload; retains its native structure so downstream
   *     formatters can render it in the shape the sync path already produces
   * @param tookMillis elapsed execution time
   */
  record Explain(ExplainResponse response, long tookMillis) implements QueryResult {

    public Explain {
      Objects.requireNonNull(response, "response must not be null");
      if (tookMillis < 0) {
        throw new IllegalArgumentException("tookMillis must not be negative");
      }
    }
  }

  /**
   * Adapts a callback-style {@link QueryResponse} into a {@link Rows} result. Convenience for
   * engines that already produce {@code QueryResponse} instances.
   */
  static Rows of(QueryResponse response, long tookMillis) {
    Objects.requireNonNull(response, "response must not be null");
    return new Rows(
        response.getSchema(),
        response.getResults(),
        response.getCursor(),
        response.getWarnings(),
        tookMillis);
  }

  /** Adapts a callback-style {@link ExplainResponse} into an {@link Explain} result. */
  static Explain of(ExplainResponse response, long tookMillis) {
    return new Explain(response, tookMillis);
  }
}
