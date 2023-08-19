/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response.format;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.protocol.response.QueryResult;

/**
 * JDBC formatter that formats both normal or error response exactly same way as legacy code to
 * avoid impact on client side. The only difference is a new "version" that indicates the response
 * was produced by new query engine.
 */
public class JdbcResponseFormatter extends JsonResponseFormatter<QueryResult> {

  public JdbcResponseFormatter(Style style) {
    super(style);
  }

  @Override
  protected Object buildJsonObject(QueryResult response) {
    JdbcResponse.JdbcResponseBuilder json = JdbcResponse.builder();

    // Fetch schema and data rows
    response.getSchema().getColumns().forEach(col -> json.column(fetchColumn(col)));
    json.datarows(fetchDataRows(response));

    // Populate other fields
    json.total(response.size())
        .size(response.size())
        .status(200);
    if (!response.getCursor().equals(Cursor.None)) {
      json.cursor(response.getCursor().toString());
    }

    return json.build();
  }

  @Override
  public String format(Throwable t) {
    int status = getStatus(t);
    Error error = new Error(
        t.getClass().getSimpleName(),
        "Invalid Query",
        t.getLocalizedMessage());
    return jsonify(new JdbcErrorResponse(error, status));
  }

  private Column fetchColumn(Schema.Column col) {
    return new Column(col.getName(), col.getAlias(), convertToLegacyType(col.getExprType()));
  }

  /**
   * Convert type that exists in both legacy and new engine but has different name.
   * Return old type name to avoid breaking impact on client-side.
   */
  private String convertToLegacyType(ExprType type) {
    return type.legacyTypeName().toLowerCase();
  }

  private Object[][] fetchDataRows(QueryResult response) {
    Object[][] rows = new Object[response.size()][];
    int i = 0;
    for (Object[] values : response) {
      rows[i++] = values;
    }
    return rows;
  }

  private int getStatus(Throwable t) {
    return (t instanceof SyntaxCheckException
        || t instanceof QueryEngineException) ? 400 : 503;
  }

  /**
   * org.json requires these inner data classes be public (and static)
   */
  @Builder
  @Getter
  public static class JdbcResponse {
    @Singular("column")
    private final List<Column> schema;
    private final Object[][] datarows;
    private final long total;
    private final long size;
    private final int status;

    private final String cursor;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Column {
    private final String name;
    private final String alias;
    private final String type;
  }

  @RequiredArgsConstructor
  @Getter
  public static class JdbcErrorResponse {
    private final Error error;
    private final int status;
  }

  @RequiredArgsConstructor
  @Getter
  public static class Error {
    private final String type;
    private final String reason;
    private final String details;
  }

}
