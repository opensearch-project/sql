package org.opensearch.sql.catalog;

import java.util.Optional;
import java.util.Set;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Catalog Service defines api for
 * providing and managing storage engines and execution engines
 * for all the catalogs.
 * The storage and execution indirectly make connections to the underlying datastore catalog.
 */
public interface CatalogService {

  Optional<StorageEngine> getStorageEngine(String catalog);

  Set<String> getCatalogs();

}
