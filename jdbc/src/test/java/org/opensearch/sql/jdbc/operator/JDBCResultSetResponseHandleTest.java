/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
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
import static org.opensearch.sql.jdbc.operator.JDBCResultSetResponseHandle.jdbcTypeToCoreType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.executor.ExecutionEngine;

@ExtendWith(MockitoExtension.class)
class JDBCResultSetResponseHandleTest {
  @Mock private ResultSetMetaData resultSetMetaData;
  @Mock private ResultSet resultSet;

  @BeforeEach
  public void setup() throws SQLException {
    lenient().when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    lenient().when(resultSetMetaData.getColumnCount()).thenReturn(1);
    lenient().when(resultSetMetaData.getColumnType(1)).thenReturn(Types.BOOLEAN);
    lenient().when(resultSetMetaData.getColumnName(1)).thenReturn("name");
  }

  @Test
  public void schema() {
    ExecutionEngine.Schema schema = new JDBCResultSetResponseHandle(resultSet).schema();
    assertEquals(1, schema.getColumns().size());
    assertEquals(
        new ExecutionEngine.Schema.Column("name", "name", BOOLEAN), schema.getColumns().get(0));
  }

  @Test
  public void close() throws SQLException {
    new JDBCResultSetResponseHandle(resultSet).close();

    verify(resultSet).close();
  }

  @Test
  public void hasNext() throws SQLException {
    when(resultSet.next()).thenReturn(true);
    assertTrue(new JDBCResultSetResponseHandle(resultSet).hasNext());
  }

  @Test
  public void next() throws SQLException {
    when(resultSet.getObject(1)).thenReturn(true);

    ExprValue value = new JDBCResultSetResponseHandle(resultSet).next();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(true, value.tupleValue().get("name").value());
  }

  @Test
  public void testJdbcTypeToCoreType() {
    // Test for all known types
    assertEquals(BOOLEAN, jdbcTypeToCoreType(Types.BIT));
    assertEquals(BOOLEAN, jdbcTypeToCoreType(Types.BOOLEAN));
    assertEquals(BYTE, jdbcTypeToCoreType(Types.TINYINT));
    assertEquals(SHORT, jdbcTypeToCoreType(Types.SMALLINT));
    assertEquals(INTEGER, jdbcTypeToCoreType(Types.INTEGER));
    assertEquals(LONG, jdbcTypeToCoreType(Types.BIGINT));
    assertEquals(DOUBLE, jdbcTypeToCoreType(Types.REAL));
    assertEquals(DOUBLE, jdbcTypeToCoreType(Types.FLOAT));
    assertEquals(DOUBLE, jdbcTypeToCoreType(Types.DOUBLE));
    assertEquals(DOUBLE, jdbcTypeToCoreType(Types.NUMERIC));
    assertEquals(DOUBLE, jdbcTypeToCoreType(Types.DECIMAL));
    assertEquals(STRING, jdbcTypeToCoreType(Types.CHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.NCHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.VARCHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.NVARCHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.LONGVARCHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.LONGNVARCHAR));
    assertEquals(STRING, jdbcTypeToCoreType(Types.BINARY));
    assertEquals(STRING, jdbcTypeToCoreType(Types.VARBINARY));
    assertEquals(STRING, jdbcTypeToCoreType(Types.LONGVARBINARY));
    assertEquals(DATE, jdbcTypeToCoreType(Types.DATE));
    assertEquals(TIME, jdbcTypeToCoreType(Types.TIME));
    assertEquals(TIMESTAMP, jdbcTypeToCoreType(Types.TIMESTAMP));
    assertEquals(STRING, jdbcTypeToCoreType(Types.ARRAY));
    assertEquals(STRING, jdbcTypeToCoreType(Types.JAVA_OBJECT));
    assertEquals(STRING, jdbcTypeToCoreType(Types.STRUCT));

    // Test for unknown type
    assertEquals(null, jdbcTypeToCoreType(999));
  }

  @Test
  public void constructException() throws SQLException {
    when(resultSetMetaData.getColumnCount()).thenThrow(SQLException.class);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> new JDBCResultSetResponseHandle(resultSet));
    assertTrue(exception.getCause() instanceof SQLException);
  }

  @Test
  public void jdbcRowExprValueTypeException() {
    JDBCResultSetResponseHandle.JDBCRowExprValue exprValue =
        new JDBCResultSetResponseHandle.JDBCRowExprValue(true);
    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, exprValue::type);
    assertEquals("[BUG] - invalid to get type on JDBCRowExprValue", exception.getMessage());
  }

  @Test
  public void jdbcRowExprValueCompareToException() {
    JDBCResultSetResponseHandle.JDBCRowExprValue exprValue =
        new JDBCResultSetResponseHandle.JDBCRowExprValue(true);
    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, () -> exprValue.compareTo(LITERAL_TRUE));
    assertEquals("[BUG] - invalid to compare on JDBCRowExprValue", exception.getMessage());
  }
}
