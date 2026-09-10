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
 * <p>Narrowing has to happen here rather than on the table, because a table's schema is derived
 * from whatever its name resolves to: by the time one exists, the mapping merge this is meant to
 * reduce has already run. Kept separate from {@link StorageEngine} so engines that cannot prune --
 * most of them -- neither implement nor know about it, and so probing for an index list stays in
 * the module that owns a client to probe with.
 */
public interface SupportsIndexPruning {

  /**
   * Get a {@link Table} that reads only what can hold data in {@code bounds}.
   *
   * <p>Best-effort: declining to narrow is always correct, so an engine that cannot tell the parts
   * apart, or fails trying, returns the same table {@link StorageEngine#getTable} would.
   */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName, TimeBounds bounds);
}
