/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.executor.TimeBounds;

/**
 * A {@link StorageEngine} that can resolve a table already narrowed to the parts of it able to hold
 * data in a time range.
 *
 * <p>Separate from {@link StorageEngine} so engines that cannot prune neither implement nor know
 * about it, and so the probe stays in the module that owns a client to issue it.
 */
public interface SupportsIndexPruning {

  /**
   * Get a {@link Table} reading only what can hold data in {@code bounds}. Best-effort: an engine
   * that cannot narrow, or fails trying, returns what {@link StorageEngine#getTable} would.
   */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName, TimeBounds bounds);
}
