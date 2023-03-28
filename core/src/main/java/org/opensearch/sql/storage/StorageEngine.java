/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.FunctionResolver;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName);

  /**
   * Get list of datasource related functions.
   *
   * @return FunctionResolvers of datasource functions.
   */
  default Collection<FunctionResolver> getFunctions() {
    return Collections.emptyList();
  }

  default TableScanOperator getTableScan(String indexName, String scrollId) {
    String error = String.format("%s.getTableScan needs to be implemented", getClass());
    throw new UnsupportedOperationException(error);
  }
}
