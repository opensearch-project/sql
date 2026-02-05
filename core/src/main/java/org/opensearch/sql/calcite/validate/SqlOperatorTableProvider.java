/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.sql.SqlOperatorTable;

/**
 * Provider interface for obtaining SqlOperatorTable instances.
 *
 * <p>This interface breaks the circular dependency between core and opensearch modules by allowing
 * the opensearch module to provide its operator table implementation to the core module through
 * dependency injection.
 */
@FunctionalInterface
public interface SqlOperatorTableProvider {
  /**
   * Gets the SQL operator table to use for validation and query processing.
   *
   * @return SqlOperatorTable instance
   */
  SqlOperatorTable getOperatorTable();
}
