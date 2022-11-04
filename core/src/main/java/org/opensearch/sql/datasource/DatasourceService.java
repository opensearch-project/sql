/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import java.util.Set;
import org.opensearch.sql.datasource.model.Datasource;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Catalog Service manages catalogs.
 */
public interface DatasourceService {

  /**
   * Returns all datasource objects.
   *
   * @return Datasource Datasources.
   */
  Set<Datasource> getDatasources();

  /**
   * Returns Catalog with corresponding to the datasource name.
   *
   * @param datasourceName Name of the datasource.
   * @return Datasource datasource.
   */
  Datasource getDatasource(String datasourceName);

  /**
   * Default opensearch engine is not defined in datasources.json.
   * So the registration of default datasource happens separately.
   *
   * @param storageEngine StorageEngine.
   */
  void registerDefaultOpenSearchDatasource(StorageEngine storageEngine);

}
