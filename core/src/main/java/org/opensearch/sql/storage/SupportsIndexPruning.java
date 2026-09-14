/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.executor.TimeBounds;

/** A {@link StorageEngine} that can resolve a table narrowed to a time range. */
public interface SupportsIndexPruning {

  /**
   * Get a {@link Table} reading only what can hold data in {@code bounds}. Best-effort: returns
   * what {@link StorageEngine#getTable} would when it cannot narrow.
   */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName, TimeBounds bounds);
}
