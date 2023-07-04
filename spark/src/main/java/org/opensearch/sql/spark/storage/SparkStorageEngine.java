/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import java.util.Collection;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.resolver.SparkSqlTableFunctionResolver;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * Spark storage engine implementation.
 */
@RequiredArgsConstructor
public class SparkStorageEngine implements StorageEngine {
  private final SparkClient sparkClient;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(
        new SparkSqlTableFunctionResolver(sparkClient));
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    return null;
  }
}
