/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.runner.connection;

import java.util.List;
import org.opensearch.sql.correctness.runner.resultset.DBResult;

/** Abstraction for different databases. */
public interface DBConnection {

  /**
   * @return database name
   */
  String getDatabaseName();

  /** Connect to database by opening a connection. */
  void connect();

  /**
   * Create table with the schema.
   *
   * @param tableName table name
   * @param schema schema json in OpenSearch mapping format
   */
  void create(String tableName, String schema);

  /**
   * Insert batch of data to database.
   *
   * @param tableName table name
   * @param columnNames column names
   * @param batch batch of rows
   */
  void insert(String tableName, String[] columnNames, List<Object[]> batch);

  /**
   * Fetch data from database.
   *
   * @param query SQL query
   * @return result set
   */
  DBResult select(String query);

  /**
   * Drop table.
   *
   * @param tableName table name
   */
  void drop(String tableName);

  /** Close the database connection. */
  void close();
}
