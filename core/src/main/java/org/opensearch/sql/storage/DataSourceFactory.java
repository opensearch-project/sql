/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

/**
 * {@link DataSourceFactory} is used to create {@link DataSource} from {@link DataSourceMetadata}.
 * Each data source define {@link DataSourceFactory} and register to {@link DataSourceService}.
 * {@link DataSourceFactory} is one instance per JVM . Each {@link DataSourceType} mapping to one
 * {@link DataSourceFactory}.
 */
public interface DataSourceFactory {
  /**
   * Get {@link DataSourceType}.
   */
  DataSourceType getDataSourceType();

  /**
   * Create {@link DataSource}.
   */
  DataSource createDataSource(DataSourceMetadata metadata);

}
