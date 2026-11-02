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
  private Function<List<ExprValue>, Schema> schema;

  /** Starts collecting; {@code schema} builds the schema for a list of rows. */
  public synchronized void open(Function<List<ExprValue>, Schema> schema) {
    this.schema = schema;
    rows.clear();
  }

  public synchronized void add(ExprValue row) {
    if (schema != null) {
      rows.add(row);
    }
  }

  /** Drops the rows, e.g. when execution fails or falls back to another engine. */
  public synchronized void discard() {
    schema = null;
    rows.clear();
  }

  /** Empty until the first row. */
  public Optional<QueryResult.Rows> snapshot() {
    Function<List<ExprValue>, Schema> schemaOf;
    List<ExprValue> copy;
    synchronized (this) {
      if (schema == null || rows.isEmpty()) {
        return Optional.empty();
      }
      schemaOf = schema;
      copy = List.copyOf(rows);
    }
    return Optional.of(new QueryResult.Rows(schemaOf.apply(copy), copy, Cursor.None, List.of(), 0));
  }
}
