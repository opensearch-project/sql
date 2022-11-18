/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import java.util.Set;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/**
 * DataSource Service manage {@link DataSource}.
 */
public interface DataSourceService {

  /**
   * Returns all DataSource objects.
   *
   * @return set of {@link DataSource}.
   */
  Set<DataSource> getDataSources();

  /**
   * Returns {@link DataSource} with corresponding to the DataSource name.
   *
   * @param dataSourceName Name of the {@link DataSource}.
   * @return {@link DataSource}.
   */
  DataSource getDataSource(String dataSourceName);

  /**
   * Register {@link DataSource} defined by {@link DataSourceMetadata}.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void addDataSource(DataSourceMetadata dataSourceMetadata);

  /**
   * remove all the registered {@link DataSource}.
   */
  void clear();
}
