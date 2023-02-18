/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.operator;

import static org.opensearch.sql.jdbc.parser.PropertiesParser.DRIVER;
import static org.opensearch.sql.jdbc.parser.PropertiesParser.URL;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.TableScanOperator;

@RequiredArgsConstructor
public class JDBCQueryOperator extends TableScanOperator {

  private final String url;

  private final String driver;

  private final String sqlQuery;

  private final Properties properties;

  private Connection connection;

  private Statement statement;

  private ResultSet resultSet;

  private JDBCResponseHandle jdbcResponse;

  /**
   * constructor.
   */
  public JDBCQueryOperator(String sqlQuery, Properties properties) {
    this.sqlQuery = sqlQuery;
    this.properties = properties;
    this.url = properties.getProperty(URL);
    this.driver = properties.getProperty(DRIVER);
  }

  @Override
  public String explain() {
    return String.format(Locale.ROOT, "jdbc(%s)", sqlQuery);
  }

  @Override
  public void open() {
    AccessController.doPrivileged((PrivilegedAction<List<Void>>) () -> {
      try {
        Class.forName(driver);
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        resultSet = statement.executeQuery(sqlQuery);
        jdbcResponse = new JDBCResponseHandle(resultSet.getMetaData());
        return null;
      } catch (SQLException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Override
  public void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    try {
      return resultSet.next();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ExprValue next() {
    return jdbcResponse.parse(resultSet);
  }

  /**
   * Schema is determined at query execution time.
   */
  @Override
  public ExecutionEngine.Schema schema() {
    return jdbcResponse.schema();
  }
}
