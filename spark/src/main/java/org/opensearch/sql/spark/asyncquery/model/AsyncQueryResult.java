package org.opensearch.sql.spark.asyncquery.model;

import java.util.Collection;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.QueryResult;

/** AsyncQueryResult for async query APIs. */
public class AsyncQueryResult extends QueryResult {

  @Getter private final String status;

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor) {
    super(schema, exprValues, cursor);
    this.status = status;
  }

  public AsyncQueryResult(
      String status, ExecutionEngine.Schema schema, Collection<ExprValue> exprValues) {
    super(schema, exprValues);
    this.status = status;
  }
}
