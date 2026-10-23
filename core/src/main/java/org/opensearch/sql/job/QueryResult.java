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
 * Wire-protocol response for a query submission.
 *
 * <p>{@link Rows} and {@link Explain} are engine-produced payloads stored in {@link
 * QueryJobStatus#result()}. {@link Running} is produced only at the submit-time wait boundary when
 * the {@code wait_for_completion_timeout} budget expires; it is never persisted on a {@code
 * QueryJobStatus} and carries just the id the client polls with.
 */
public sealed interface QueryResult
    permits QueryResult.Rows, QueryResult.Explain, QueryResult.Running {

  /** Elapsed execution time on the owner node; {@code 0} for {@link Running}. */
  long tookMillis();

  record Rows(
      Schema schema, List<ExprValue> rows, Cursor cursor, List<Warning> warnings, long tookMillis)
      implements QueryResult {

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

  record Explain(ExplainResponse response, long tookMillis) implements QueryResult {

    public Explain {
      Objects.requireNonNull(response, "response must not be null");
      if (tookMillis < 0) {
        throw new IllegalArgumentException("tookMillis must not be negative");
      }
    }
  }

  /** Wait budget expired; the client polls {@link #id()} to observe the terminal outcome. */
  record Running(QueryJobId id) implements QueryResult {

    public Running {
      Objects.requireNonNull(id, "id must not be null");
    }

    @Override
    public long tookMillis() {
      return 0L;
    }
  }

  static Rows of(QueryResponse response, long tookMillis) {
    Objects.requireNonNull(response, "response must not be null");
    return new Rows(
        response.getSchema(),
        response.getResults(),
        response.getCursor(),
        response.getWarnings(),
        tookMillis);
  }

  static Explain of(ExplainResponse response, long tookMillis) {
    return new Explain(response, tookMillis);
  }
}
