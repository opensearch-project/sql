/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
@AllArgsConstructor
public class OpenSearchSchema extends AbstractSchema {
  public static final String OPEN_SEARCH_SCHEMA_NAME = "OpenSearch";

  private final DataSourceService dataSourceService;

  private final Map<String, Table> tableMap = new HashMap<>();

  public void registerTable(QualifiedName qualifiedName) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(dataSourceService, qualifiedName.getParts());
    org.opensearch.sql.storage.Table table =
        dataSourceService
            .getDataSource(nameResolver.getDataSourceName())
            .getStorageEngine()
            .getTable(
                new DataSourceSchemaName(
                    nameResolver.getDataSourceName(), nameResolver.getSchemaName()),
                nameResolver.getIdentifierName());
    tableMap.put(qualifiedName.toString(), (org.apache.calcite.schema.Table) table);
  }
}
