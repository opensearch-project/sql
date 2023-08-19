/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.pagination.CanPaginateVisitor;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionResolver;

/** Storage engine for different storage to provide data access API implementation. */
public interface StorageEngine {

  /** Get {@link Table} from storage engine. */
  Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName);

  /**
   * Get list of datasource related functions.
   *
   * @return FunctionResolvers of datasource functions.
   */
  default Collection<FunctionResolver> getFunctions() {
    return List.of();
  }

  default Analyzer getAnalyzer(DataSourceService dataSourceService, BuiltinFunctionRepository repository) {
    return null;
  }

  default CanPaginateVisitor getPaginationAnalyzer() {
    return null;
  }
}
