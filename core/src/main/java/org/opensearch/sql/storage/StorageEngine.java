/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import org.opensearch.sql.ddl.table.Schema;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(String name);

  /**
   * Add new table to the storage engine.
   */
  boolean addTable(Schema schema);
}
