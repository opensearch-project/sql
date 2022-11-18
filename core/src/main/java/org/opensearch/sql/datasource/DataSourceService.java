/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import java.util.Set;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.storage.StorageEngine;

/**
 * DataSource Service manages datasources.
 */
public interface DataSourceService {

  /**
   * Returns all datasource objects.
   *
   * @return DataSource datasources.
   */
  Set<DataSource> getDataSources();

  /**
   * Returns DataSource with corresponding to the datasource name.
   *
   * @param dataSourceName Name of the datasource.
   * @return DataSource datasource.
   */
  DataSource getDataSource(String dataSourceName);

  /**
   * Default opensearch engine is not defined in datasources config.
   * So the registration of default datasource happens separately.
   *
   * @param storageEngine StorageEngine.
   */
  void registerDefaultOpenSearchDataSource(StorageEngine storageEngine);

}
