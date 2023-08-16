/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.protocol.response;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.executor.pagination.Cursor;

/**
 * Query response that encapsulates query results and isolate {@link ExprValue}
 * related from formatter implementation.
 */
@RequiredArgsConstructor
public class QueryResult implements Iterable<Object[]> {

  @Getter
  private final ExecutionEngine.Schema schema;

  /**
   * Results which are collection of expression.
   */
  private final Collection<ExprValue> exprValues;

  @Getter
  private final Cursor cursor;

  public QueryResult(ExecutionEngine.Schema schema, Collection<ExprValue> exprValues) {
    this(schema, exprValues, Cursor.None);
  }

  /**
   * size of results.
   * @return size of results
   */
  public int size() {
    return exprValues.size();
  }

  /**
   * Parse column name from results.
   *
   * @return mapping from column names to its expression type.
   *        note that column name could be original name or its alias if any.
   */
  public Map<String, String> columnNameTypes() {
    Map<String, String> colNameTypes = new LinkedHashMap<>();
    schema.getColumns().forEach(column -> colNameTypes.put(
        getColumnName(column),
        column.getExprType().typeName().toLowerCase(Locale.ROOT)));
    return colNameTypes;
  }

  @Override
  public Iterator<Object[]> iterator() {
    // Any chance to avoid copy for json response generation?
    return exprValues.stream()
        .map(ExprValueUtils::getTupleValue)
        .map(Map::values)
        .map(this::convertExprValuesToValues)
        .iterator();
  }

  private String getColumnName(Column column) {
    return (column.getAlias() != null) ? column.getAlias() : column.getName();
  }

  private Object[] convertExprValuesToValues(Collection<ExprValue> exprValues) {
    return exprValues
        .stream()
        .map(ExprValue::value)
        .toArray(Object[]::new);
  }
}
