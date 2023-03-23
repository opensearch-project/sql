/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Handle JDBC {@link ResultSet} response.
 */
public class JDBCResultSetResponseHandle implements JDBCResponseHandle {

  private final ResultSet resultSet;

  private final List<ColumnHandle> columnHandleList;

  /**
   * constructor.
   */
  public JDBCResultSetResponseHandle(ResultSet resultSet) {
    this.resultSet = resultSet;
    try {
      ResultSetMetaData metaData = resultSet.getMetaData();
      columnHandleList = new ArrayList<>();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        // the default type is STRING.
        ExprType exprType = jdbcTypeToCoreType(metaData.getColumnType(i));
        columnHandleList.add(new ColumnHandle(i, metaData.getColumnName(i), exprType));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    resultSet.close();
  }

  @Override
  public boolean hasNext() throws SQLException {
    return resultSet.next();
  }

  @Override
  public ExprValue next() throws SQLException {
    LinkedHashMap<String, ExprValue> result = new LinkedHashMap<>();
    for (ColumnHandle columnHandle : columnHandleList) {
      result.put(columnHandle.getName(), columnHandle.parse(resultSet));
    }
    return new ExprTupleValue(result);
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(
        columnHandleList.stream()
            .map(c -> new ExecutionEngine.Schema.Column(c.getName(), c.getName(), c.getType()))
            .collect(Collectors.toList()));
  }

  @Getter
  @RequiredArgsConstructor
  static class ColumnHandle {
    private final int index;

    private final String name;

    private final ExprType type;

    ExprValue parse(ResultSet rs) throws SQLException {
      return new JDBCRowExprValue(rs.getObject(index));
    }
  }

  @RequiredArgsConstructor
  static class JDBCRowExprValue implements ExprValue {

    private final Object value;

    @Override
    public Object value() {
      return value;
    }

    @Override
    public ExprType type() {
      throw new ExpressionEvaluationException("[BUG] - invalid to get type on JDBCRowExprValue");
    }

    @Override
    public int compareTo(ExprValue o) {
      throw new ExpressionEvaluationException("[BUG] - invalid to compare on JDBCRowExprValue");
    }
  }

  /**
   * Mapping JDBC type to Core Engine {@link ExprType}.
   *
   * @param type jdbc type.
   * @return ExprType.
   */
  static ExprType jdbcTypeToCoreType(int type) {
    switch (type) {
      case Types.BIT:
      case Types.BOOLEAN:
        return BOOLEAN;

      case Types.TINYINT:
        return BYTE;

      case Types.SMALLINT:
        return SHORT;

      case Types.INTEGER:
        return INTEGER;

      case Types.BIGINT:
        return LONG;

      case Types.REAL:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        return DOUBLE;

      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
      case Types.NVARCHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return STRING;

      case Types.DATE:
        return DATE;

      case Types.TIME:
        return TIME;

      case Types.TIMESTAMP:
        return TIMESTAMP;

      // we assume the result is json encoded string. refer https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.0.0.2/ds_Hive/jdbc-hs2.html,
      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.STRUCT:
        return STRING;
      default:
        return STRING;
    }
  }
}
