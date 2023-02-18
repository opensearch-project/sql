/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
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
import static org.opensearch.sql.jdbc.operator.JDBCResponseHandle.jdbcTypeToCoreType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.executor.ExecutionEngine;

@ExtendWith(MockitoExtension.class)
class JDBCResponseHandleTest {
  @Mock
  private ResultSetMetaData resultSetMetaData;
  @Mock
  private ResultSet resultSet;

  @Test
  public void schema() throws SQLException {
    when(resultSetMetaData.getColumnCount()).thenReturn(1);
    when(resultSetMetaData.getColumnType(1)).thenReturn(Types.BOOLEAN);
    when(resultSetMetaData.getColumnName(1)).thenReturn("name");

    ExecutionEngine.Schema schema = new JDBCResponseHandle(resultSetMetaData).schema();
    assertEquals(1, schema.getColumns().size());
    assertEquals(
        new ExecutionEngine.Schema.Column("name", "name", BOOLEAN), schema.getColumns().get(0));
  }

  @Test
  public void parse() throws SQLException {
    when(resultSetMetaData.getColumnCount()).thenReturn(1);
    when(resultSetMetaData.getColumnType(1)).thenReturn(Types.BOOLEAN);
    when(resultSetMetaData.getColumnName(1)).thenReturn("name");
    when(resultSet.getObject(1)).thenReturn(true);

    ExprValue value = new JDBCResponseHandle(resultSetMetaData).parse(resultSet);
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(true, value.tupleValue().get("name").value());
  }

  @Test
  public void testJdbcTypeToCoreType() {
    // Test for all known types
    assertEquals(Optional.of(BOOLEAN), jdbcTypeToCoreType(Types.BIT));
    assertEquals(Optional.of(BOOLEAN), jdbcTypeToCoreType(Types.BOOLEAN));
    assertEquals(Optional.of(BYTE), jdbcTypeToCoreType(Types.TINYINT));
    assertEquals(Optional.of(SHORT), jdbcTypeToCoreType(Types.SMALLINT));
    assertEquals(Optional.of(INTEGER), jdbcTypeToCoreType(Types.INTEGER));
    assertEquals(Optional.of(LONG), jdbcTypeToCoreType(Types.BIGINT));
    assertEquals(Optional.of(DOUBLE), jdbcTypeToCoreType(Types.REAL));
    assertEquals(Optional.of(DOUBLE), jdbcTypeToCoreType(Types.FLOAT));
    assertEquals(Optional.of(DOUBLE), jdbcTypeToCoreType(Types.DOUBLE));
    assertEquals(Optional.of(DOUBLE), jdbcTypeToCoreType(Types.NUMERIC));
    assertEquals(Optional.of(DOUBLE), jdbcTypeToCoreType(Types.DECIMAL));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.CHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.NCHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.VARCHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.NVARCHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.LONGVARCHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.LONGNVARCHAR));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.BINARY));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.VARBINARY));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.LONGVARBINARY));
    assertEquals(Optional.of(DATE), jdbcTypeToCoreType(Types.DATE));
    assertEquals(Optional.of(TIME), jdbcTypeToCoreType(Types.TIME));
    assertEquals(Optional.of(TIMESTAMP), jdbcTypeToCoreType(Types.TIMESTAMP));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.ARRAY));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.JAVA_OBJECT));
    assertEquals(Optional.of(STRING), jdbcTypeToCoreType(Types.STRUCT));

    // Test for unknown type
    assertEquals(Optional.empty(), jdbcTypeToCoreType(999));
  }

  @Test
  public void constructException() throws SQLException {
    when(resultSetMetaData.getColumnCount()).thenThrow(SQLException.class);

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> new JDBCResponseHandle(resultSetMetaData));
    assertTrue(exception.getCause() instanceof SQLException);
  }

  @Test
  public void jdbcRowExprValueTypeException() {
    JDBCResponseHandle.JDBCRowExprValue exprValue = new JDBCResponseHandle.JDBCRowExprValue(true);
    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, exprValue::type);
    assertEquals("[BUG] - invalid to get type on JDBCRowExprValue", exception.getMessage());
  }

  @Test
  public void jdbcRowExprValueCompareToException() {
    JDBCResponseHandle.JDBCRowExprValue exprValue = new JDBCResponseHandle.JDBCRowExprValue(true);
    ExpressionEvaluationException exception =
        assertThrows(ExpressionEvaluationException.class, () -> exprValue.compareTo(LITERAL_TRUE));
    assertEquals("[BUG] - invalid to compare on JDBCRowExprValue", exception.getMessage());
  }

  @Test
  public void columnHandleParseException() throws SQLException {
    when(resultSet.getObject(anyInt())).thenThrow(SQLException.class);

    RuntimeException exception = assertThrows(
        RuntimeException.class,
        () -> new JDBCResponseHandle.ColumnHandle(1, "name", BOOLEAN).parse(resultSet)
    );
    assertTrue(exception.getCause() instanceof SQLException);
  }
}
