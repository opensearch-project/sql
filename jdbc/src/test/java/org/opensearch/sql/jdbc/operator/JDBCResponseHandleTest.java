/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JDBCResponseHandleTest {

  @Mock
  private Statement statementMock;

  @Mock
  private ResultSet resultSetMock;

  @Mock
  private ResultSetMetaData resultSetMetaDataMock;

  private final String sqlQuery = "SELECT * FROM my_table";

  @BeforeEach
  public void setup() throws SQLException {
    lenient().when(statementMock.execute(anyString())).thenReturn(true);
    lenient().when(statementMock.getResultSet()).thenReturn(resultSetMock);
    lenient().when(resultSetMock.getMetaData()).thenReturn(resultSetMetaDataMock);
    lenient().when(resultSetMetaDataMock.getColumnCount()).thenReturn(0);
  }

  @Test
  public void testExecute() throws SQLException {
    assertTrue(
        JDBCResponseHandle.execute(statementMock, sqlQuery) instanceof JDBCResultSetResponseHandle);

    when(statementMock.execute(anyString())).thenReturn(false);
    assertTrue(
        JDBCResponseHandle.execute(statementMock, sqlQuery)
            instanceof JDBCUpdateCountResponseHandle);
  }
}
