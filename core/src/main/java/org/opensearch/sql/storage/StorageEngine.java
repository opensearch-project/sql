/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import java.util.Collection;
import java.util.Collections;
import org.opensearch.sql.DatasourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(DatasourceSchemaName datasourceSchemaName, String tableName);

  /**
   * Get list of datsource related functions.
   *
   * @return FunctionResolvers of datasource functions.
   */
  default Collection<FunctionResolver> getFunctions() {
    return Collections.emptyList();
  }

}
