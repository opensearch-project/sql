/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryResult;

/**
 * Rows the root of an async query has produced so far. They are final rows in final order, so a
 * snapshot is a prefix of the final result.
 */
public final class PartialRows {

  private final List<ExprValue> rows = new ArrayList<>();
  private Function<List<ExprValue>, Schema> schemaOf;
  private Schema schema;
  private QueryResult.Rows last;

  /** Starts collecting; {@code schemaOf} builds the schema from the rows so far. */
  public synchronized void open(Function<List<ExprValue>, Schema> schemaOf) {
    this.schemaOf = schemaOf;
    reset();
  }

  public synchronized void add(ExprValue row) {
    if (schemaOf != null) {
      rows.add(row);
    }
  }

  /** Drops the rows, e.g. when execution fails or falls back to another engine. */
  public synchronized void discard() {
    schemaOf = null;
    reset();
  }

  /** Empty until the first row; reused while no row is added. */
  public synchronized Optional<QueryResult.Rows> snapshot() {
    if (schemaOf == null || rows.isEmpty()) {
      return Optional.empty();
    }
    if (last == null || last.rows().size() != rows.size()) {
      List<ExprValue> copy = List.copyOf(rows);
      if (schema == null) {
        // Fixed once there is a row: only the first row decides runtime-typed columns.
        schema = schemaOf.apply(copy);
      }
      last = new QueryResult.Rows(schema, copy, Cursor.None, List.of(), 0);
    }
    return Optional.of(last);
  }

  private void reset() {
    rows.clear();
    schema = null;
    last = null;
  }
}
