/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.INFORMATION_SCHEMA_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.resolver.QueryExemplarsTableFunctionResolver;
import org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.prometheus.storage.system.PrometheusSystemTable;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;


/**
 * Prometheus storage engine implementation.
 */
@RequiredArgsConstructor
public class PrometheusStorageEngine implements StorageEngine {

  private final PrometheusClient prometheusClient;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    ArrayList<FunctionResolver> functionList = new ArrayList<>();
    functionList.add(new QueryRangeTableFunctionResolver(prometheusClient));
    functionList.add(new QueryExemplarsTableFunctionResolver(prometheusClient));
    return functionList;
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    if (isSystemIndex(tableName)) {
      return new PrometheusSystemTable(prometheusClient, dataSourceSchemaName, tableName);
    } else if (INFORMATION_SCHEMA_NAME.equals(dataSourceSchemaName.getSchemaName())) {
      return resolveInformationSchemaTable(dataSourceSchemaName, tableName);
    } else {
      return new PrometheusMetricTable(prometheusClient, tableName);
    }
  }

  private Table resolveInformationSchemaTable(DataSourceSchemaName dataSourceSchemaName,
                                              String tableName) {
    if (SystemIndexUtils.TABLE_NAME_FOR_TABLES_INFO.equals(tableName)) {
      return new PrometheusSystemTable(prometheusClient,
          dataSourceSchemaName, SystemIndexUtils.TABLE_INFO);
    } else {
      throw new SemanticCheckException(
          String.format("Information Schema doesn't contain %s table", tableName));
    }
  }


}
