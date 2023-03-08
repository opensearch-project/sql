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
   * Returns {@link DataSource} corresponding to the DataSource name.
   *
   * @param dataSourceName Name of the {@link DataSource}.
   * @return {@link DataSource}.
   */
  DataSource getDataSource(String dataSourceName);


  /**
   * Returns all dataSource Metadata objects. The returned objects won't contain
   * any of the credential info.
   *
   * @return set of {@link DataSourceMetadata}.
   */
  Set<DataSourceMetadata> getDataSourceMetadataSet();

  /**
   * Register {@link DataSource} defined by {@link DataSourceMetadata}.
   *
   * @param metadatas list of {@link DataSourceMetadata}.
   */
  void createDataSource(DataSourceMetadata... metadatas);

  /**
   * Updates {@link DataSource} corresponding to dataSourceMetadata.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void updateDataSource(DataSourceMetadata dataSourceMetadata);


  /**
   * Deletes {@link DataSource} corresponding to the DataSource name.
   *
   * @param dataSourceName name of the {@link DataSource}.
   */
  void deleteDataSource(String dataSourceName);

  /**
   * This method is to bootstrap
   * datasources during the startup of the plugin.
   */
  void bootstrapDataSources();

  /**
   * remove all the registered {@link DataSource}.
   */
  void clear();
}
