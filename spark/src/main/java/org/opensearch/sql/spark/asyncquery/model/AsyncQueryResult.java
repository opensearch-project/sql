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
  @Getter private final String error;
  @Getter private final String sessionId;

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      String error,
      String sessionId) {
    super(schema, exprValues, cursor);
    this.status = status;
    this.error = error;
    this.sessionId = sessionId;
  }

  public AsyncQueryResult(
      String status,
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      String error,
      String sessionId) {
    super(schema, exprValues);
    this.status = status;
    this.error = error;
    this.sessionId = sessionId;
  }
}
