/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

public class JDBCStorageFactory implements DataSourceFactory {
  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.JDBC;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(metadata.getName(), DataSourceType.JDBC, new JDBCStorageEngine(metadata));
  }
}
