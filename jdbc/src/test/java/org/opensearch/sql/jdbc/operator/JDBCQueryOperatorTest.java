/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.DRIVER;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.URL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JDBCQueryOperatorTest {
  @Mock private Connection connectionMock;

  @Mock private Statement statementMock;

  @Mock private JDBCResponseHandle jdbcResponseHandleMock;

  private MockedStatic<JDBCResponseHandle> staticJDBCResponseHandleMock;

  private MockedStatic<DriverManager> driverManagerMock;

  private JDBCQueryOperator operator;

  private final String sqlQuery = "SELECT * FROM my_table";

  private final Properties properties = new Properties();

  @BeforeEach
  public void setup() throws SQLException {
    properties.put(URL, "jdbc:hive2://localhost:10000/default");
    properties.put(DRIVER, "org.apache.hive.jdbc.HiveDriver");
    operator = new JDBCQueryOperator(sqlQuery, properties);

    driverManagerMock = Mockito.mockStatic(DriverManager.class);
    staticJDBCResponseHandleMock = Mockito.mockStatic(JDBCResponseHandle.class);

    lenient().when(connectionMock.createStatement()).thenReturn(statementMock);
    lenient().when(statementMock.execute(anyString())).thenReturn(true);

    driverManagerMock
        .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
        .thenReturn(connectionMock);
    staticJDBCResponseHandleMock
        .when(() -> JDBCResponseHandle.execute(any(Statement.class), anyString()))
        .thenReturn(jdbcResponseHandleMock);
  }

  @AfterEach
  public void clean() {
    driverManagerMock.close();
    staticJDBCResponseHandleMock.close();
  }

  @Test
  public void openAndClose() throws Exception {
    operator.open();
    verify(connectionMock).createStatement();

    operator.close();
    verify(jdbcResponseHandleMock).close();
    verify(statementMock).close();
    verify(connectionMock).close();
  }

  @Test
  public void openConnectionException() throws SQLException {
    driverManagerMock
        .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
        .thenThrow(SQLException.class);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof SQLException);

    operator.close();
    verify(jdbcResponseHandleMock, never()).close();
    verify(statementMock, never()).close();
    verify(connectionMock, never()).close();
  }

  @Test
  public void driverNotFoundException() throws SQLException {
    properties.put(URL, "jdbc:hive2://localhost:10000/default");
    properties.put(DRIVER, "unknown driver");
    operator = new JDBCQueryOperator(sqlQuery, properties);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof ClassNotFoundException);

    operator.close();
    verify(jdbcResponseHandleMock, never()).close();
    verify(statementMock, never()).close();
    verify(connectionMock, never()).close();
  }

  @Test
  public void createStatementException() throws SQLException {
    when(connectionMock.createStatement()).thenThrow(SQLException.class);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof SQLException);

    operator.close();
    verify(jdbcResponseHandleMock, never()).close();
    verify(statementMock, never()).close();
    verify(connectionMock).close();
  }

  @Test
  public void executeException() throws SQLException {
    staticJDBCResponseHandleMock
        .when(() -> JDBCResponseHandle.execute(any(Statement.class), anyString()))
        .thenThrow(SQLNonTransientException.class);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof SQLNonTransientException);

    operator.close();
    verify(jdbcResponseHandleMock, never()).close();
    verify(statementMock).close();
    verify(connectionMock).close();
  }

  @Test
  public void closeException() throws Exception {
    operator.open();
    verify(connectionMock).createStatement();

    doThrow(SQLException.class).when(jdbcResponseHandleMock).close();
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.close());
    assertTrue(ex.getCause() instanceof SQLException);
  }

  @Test
  public void next() throws Exception {
    when(jdbcResponseHandleMock.hasNext()).thenReturn(true);

    operator.open();
    assertTrue(operator.hasNext());
    operator.next();
    verify(jdbcResponseHandleMock).next();
  }

  @Test
  public void hasNextRuntimeException() throws Exception {
    when(jdbcResponseHandleMock.hasNext()).thenThrow(SQLException.class);

    operator.open();
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.hasNext());
    assertTrue(ex.getCause() instanceof SQLException);
  }

  @Test
  public void hasNextIllegalArgumentException() throws Exception {
    when(jdbcResponseHandleMock.hasNext()).thenThrow(SQLNonTransientException.class);

    operator.open();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> operator.hasNext());
    assertTrue(ex.getCause() instanceof SQLException);
  }

  @Test
  public void schema() {
    operator.open();
    operator.schema();
    verify(jdbcResponseHandleMock).schema();
  }

  @Test
  public void explain() {
    assertEquals("jdbc(SELECT * FROM my_table)", operator.explain());
  }
}
