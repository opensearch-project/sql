/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response;

import static org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.lang.LangSpec;

/**
 * Query response that encapsulates query results and isolate {@link ExprValue} related from
 * formatter implementation.
 */
public class QueryResult implements Iterable<Object[]> {

  @Getter private final ExecutionEngine.Schema schema;

  /** Results which are collection of expression. */
  private final Collection<ExprValue> exprValues;

  @Getter private final Cursor cursor;

  private final LangSpec langSpec;

  public QueryResult(ExecutionEngine.Schema schema, Collection<ExprValue> exprValues) {
    this(schema, exprValues, Cursor.None, LangSpec.SQL_SPEC);
  }

  public QueryResult(
      ExecutionEngine.Schema schema, Collection<ExprValue> exprValues, Cursor cursor) {
    this(schema, exprValues, cursor, LangSpec.SQL_SPEC);
  }

  public QueryResult(
      ExecutionEngine.Schema schema,
      Collection<ExprValue> exprValues,
      Cursor cursor,
      LangSpec langSpec) {
    this.schema = schema;
    this.exprValues = exprValues;
    this.cursor = cursor;
    this.langSpec = langSpec;
  }

  /**
   * size of results.
   *
   * @return size of results
   */
  public int size() {
    return exprValues.size();
  }

  /**
   * Parse column name from results.
   *
   * @return mapping from column names to its expression type. note that column name could be
   *     original name or its alias if any.
   */
  public Map<String, String> columnNameTypes() {
    Map<String, String> colNameTypes = new LinkedHashMap<>();
    schema
        .getColumns()
        .forEach(
            column ->
                colNameTypes.put(
                    getColumnName(column),
                    langSpec.typeName(column.getExprType()).toLowerCase(Locale.ROOT)));
    return colNameTypes;
  }

  @Override
  public Iterator<Object[]> iterator() {
    return exprValues.stream()
        .map(ExprValueUtils::getTupleValue)
        .map(
            tuple ->
                tuple.entrySet().stream()
                    .filter(e -> !HIGHLIGHT_FIELD.equals(e.getKey()))
                    .map(e -> e.getValue().value())
                    .toArray(Object[]::new))
        .iterator();
  }

  /**
   * Extract highlight data from each result row. Each row may contain a {@code _highlight} field
   * added by {@code OpenSearchResponse.addHighlightsToBuilder()} and preserved through projection.
   * Returns a list parallel to datarows where each entry is either a map of field name to highlight
   * fragments, or null if no highlight data exists for that row.
   *
   * @return list of highlight maps, one per row
   */
  public List<Map<String, Object>> highlights() {
    return exprValues.stream()
        .map(ExprValueUtils::getTupleValue)
        .map(
            tuple -> {
              ExprValue hl = tuple.get(HIGHLIGHT_FIELD);
              if (hl == null || hl.isMissing()) {
                return null;
              }
              Map<String, Object> hlMap = new LinkedHashMap<>();
              for (Map.Entry<String, ExprValue> entry : hl.tupleValue().entrySet()) {
                hlMap.put(
                    entry.getKey(),
                    entry.getValue().collectionValue().stream()
                        .map(ExprValue::stringValue)
                        .collect(Collectors.toList()));
              }
              return (Map<String, Object>) hlMap;
            })
        .collect(Collectors.toList());
  }

  private String getColumnName(Column column) {
    return (column.getAlias() != null) ? column.getAlias() : column.getName();
  }
}
