/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import java.sql.SQLException;
import java.sql.Statement;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * handle JDBC response.
 */
public interface JDBCResponseHandle {

  /**
   * execute sql query.
   *
   * @param statement {@link Statement}
   * @param sqlQuery sql query
   * @return {@link JDBCResponseHandle}
   */
  static JDBCResponseHandle execute(Statement statement, String sqlQuery) throws SQLException {
    boolean execute = statement.execute(sqlQuery);
    return execute ? new JDBCResultSetResponseHandle(statement.getResultSet()) :
        new JDBCUpdateCountResponseHandle(statement.getUpdateCount());
  }

  /**
   * Return true if JDBC response has more result.
   */
  boolean hasNext() throws SQLException;

  /**
   * Return JDBC response as {@link ExprValue}. Attention, the method must been called when
   * hasNext return true.
   */
  ExprValue next() throws SQLException;

  /**
   * Close {@link JDBCResponseHandle}.
   */
  void close() throws SQLException;

  /**
   * Return ExecutionEngine.Schema of the JDBC response.
   */
  ExecutionEngine.Schema schema();
}
