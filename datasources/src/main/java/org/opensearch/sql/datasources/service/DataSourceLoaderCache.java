/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.service;

import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/**
 * Interface for DataSourceLoaderCache which provides methods for
 * fetch, loading and invalidating DataSource cache.
 */
public interface DataSourceLoaderCache {

  /**
   * Returns cached datasource object or loads a new one if not present.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   * @return {@link DataSource}
   */
  DataSource getOrLoadDataSource(DataSourceMetadata dataSourceMetadata);

}
