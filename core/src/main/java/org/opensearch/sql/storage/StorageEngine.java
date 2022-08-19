/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import org.opensearch.sql.ddl.Column;

import java.util.List;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(String name);

  /**
   * Create table.
   */
  default boolean addTable(String name, List<Column> columns) {
    return false;
  }
}
