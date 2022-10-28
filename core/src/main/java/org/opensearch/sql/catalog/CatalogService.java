/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.catalog;

import java.util.Set;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Catalog Service manages catalogs.
 */
public interface CatalogService {

  /**
   * Returns all catalog objects.
   *
   * @return Catalog Catalogs.
   */
  Set<Catalog> getCatalogs();

  /**
   * Returns Catalog with corresponding to the catalog name.
   *
   * @param catalogName Name of the catalog.
   * @return Catalog catalog.
   */
  Catalog getCatalog(String catalogName);

  /**
   * Default opensearch engine is not defined in catalog.json.
   * So the registration of default catalog happens separately.
   *
   * @param storageEngine StorageEngine.
   */
  void registerDefaultOpenSearchCatalog(StorageEngine storageEngine);

}
