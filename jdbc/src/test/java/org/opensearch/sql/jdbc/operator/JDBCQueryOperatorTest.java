/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
import org.opensearch.sql.data.model.ExprTupleValue;

@ExtendWith(MockitoExtension.class)
class JDBCQueryOperatorTest {
  @Mock
  private Connection connectionMock;

  @Mock
  private Statement statementMock;

  @Mock
  private ResultSet resultSetMock;

  @Mock
  private ResultSetMetaData resultSetMetaData;

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
    lenient().when(connectionMock.createStatement()).thenReturn(statementMock);
    lenient().when(statementMock.executeQuery(anyString())).thenReturn(resultSetMock);
    lenient().when(resultSetMock.getMetaData()).thenReturn(resultSetMetaData);
    lenient().when(resultSetMetaData.getColumnCount()).thenReturn(0);

    driverManagerMock
        .when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
        .thenReturn(connectionMock);
  }

  @AfterEach
  public void clean() {
    driverManagerMock.close();
  }

  @Test
  public void openAndClose() throws Exception {
    operator.open();

    verify(connectionMock).createStatement();
    verify(statementMock).executeQuery(sqlQuery);

    operator.close();
    verify(resultSetMock).close();
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
    verify(resultSetMock, never()).close();
    verify(statementMock, never()).close();
    verify(connectionMock, never()).close();
  }

  @Test
  public void createStatementException() throws SQLException {
    when(connectionMock.createStatement()).thenThrow(SQLException.class);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof SQLException);

    operator.close();
    verify(resultSetMock, never()).close();
    verify(statementMock, never()).close();
    verify(connectionMock).close();
  }

  @Test
  public void executeQueryException() throws SQLException {
    lenient().when(statementMock.executeQuery(anyString())).thenThrow(SQLException.class);
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.open());
    assertTrue(ex.getCause() instanceof SQLException);

    operator.close();
    verify(resultSetMock, never()).close();
    verify(statementMock).close();
    verify(connectionMock).close();
  }

  @Test
  public void closeException() throws Exception {
    operator.open();

    verify(connectionMock).createStatement();
    verify(statementMock).executeQuery(sqlQuery);

    doThrow(SQLException.class).when(resultSetMock).close();
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.close());
    assertTrue(ex.getCause() instanceof SQLException);
  }

  @Test
  public void next() throws Exception {
    when(resultSetMock.next()).thenReturn(true);

    operator.open();
    assertTrue(operator.hasNext());
    assertTrue(operator.next() instanceof ExprTupleValue);
  }

  @Test
  public void hasNextException() throws Exception {
    when(resultSetMock.next()).thenThrow(SQLException.class);

    operator.open();
    RuntimeException ex = assertThrows(RuntimeException.class, () -> operator.hasNext());
    assertTrue(ex.getCause() instanceof SQLException);
  }

  @Test
  public void schema() {
    operator.open();

    assertNotNull(operator.schema());
  }

  @Test
  public void explain() {
    assertEquals("jdbc(SELECT * FROM my_table)", operator.explain());
  }
}
