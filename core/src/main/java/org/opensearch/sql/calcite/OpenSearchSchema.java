/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
@AllArgsConstructor
public class OpenSearchSchema extends AbstractSchema {
  public static final String OPEN_SEARCH_SCHEMA_NAME = "OpenSearch";

  private final DataSourceService dataSourceService;

  private final Map<String, Table> tableMap =
      new HashMap<>() {
        @Override
        public Table get(Object key) {
          if (!super.containsKey(key)) {
            registerTable((String) key);
          }
          return super.get(key);
        }
      };

  public void registerTable(String name) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(dataSourceService, List.of(name.split("\\.")));
    org.opensearch.sql.storage.Table table =
        dataSourceService
            .getDataSource(nameResolver.getDataSourceName())
            .getStorageEngine()
            .getTable(
                new DataSourceSchemaName(
                    nameResolver.getDataSourceName(), nameResolver.getSchemaName()),
                nameResolver.getIdentifierName());
    tableMap.put(name, (org.apache.calcite.schema.Table) table);
  }
}
