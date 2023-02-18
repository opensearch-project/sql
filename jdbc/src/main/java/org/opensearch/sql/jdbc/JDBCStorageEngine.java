/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import java.util.Collection;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.jdbc.functions.JDBCTableFunctionResolver;
import org.opensearch.sql.jdbc.parser.PropertiesParser;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * JDBC {@link StorageEngine} only support direct query.
 */
@RequiredArgsConstructor
public class JDBCStorageEngine implements StorageEngine {

  private final DataSourceMetadata dataSourceMetadata;

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    throw new SemanticCheckException("JDBC does not support getTable operation");
  }

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(
        new JDBCTableFunctionResolver(dataSourceMetadata, new PropertiesParser()));
  }
}
