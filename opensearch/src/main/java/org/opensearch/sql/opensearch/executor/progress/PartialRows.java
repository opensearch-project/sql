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
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.job.QueryResult;

/** Rows an async query has produced so far, a prefix of its final result. */
public final class PartialRows {

  private final List<ExprValue> rows = new ArrayList<>();
  private Function<List<ExprValue>, Schema> schemaOf;
  private Schema schema;
  private List<Warning> warnings = List.of();
  private QueryResult.Rows last;

  public synchronized void open(Function<List<ExprValue>, Schema> schemaOf) {
    this.schemaOf = schemaOf;
    reset();
  }

  public synchronized void add(ExprValue row) {
    if (schemaOf != null) {
      rows.add(row);
    }
  }

  public synchronized void warnings(List<Warning> warnings) {
    if (schemaOf != null && !warnings.equals(this.warnings)) {
      this.warnings = List.copyOf(warnings);
      last = null;
    }
  }

  public synchronized void discard() {
    schemaOf = null;
    reset();
  }

  public synchronized Optional<QueryResult.Rows> snapshot() {
    if (schemaOf == null || rows.isEmpty()) {
      return Optional.empty();
    }
    if (last == null || last.rows().size() != rows.size()) {
      List<ExprValue> copy = List.copyOf(rows);
      if (schema == null) {
        // Only the first row decides runtime-typed columns.
        schema = schemaOf.apply(copy);
      }
      last = new QueryResult.Rows(schema, copy, Cursor.None, warnings, 0);
    }
    return Optional.of(last);
  }

  private void reset() {
    rows.clear();
    schema = null;
    warnings = List.of();
    last = null;
  }
}
