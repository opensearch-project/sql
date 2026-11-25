/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Schema;
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

  private final Map<String, Table> tableMap =
      new HashMap<>() {
        @Override
        public Table get(Object key) {
          if (!super.containsKey(key)) {
            registerTable(new QualifiedName((String) key));
          }
          return super.get(key);
        }
      };

  private final Map<String, Schema> subSchemaMap =
      new HashMap<>() {
        @Override
        public Schema get(Object key) {
          if (!super.containsKey(key)) {
            String dsName = (String) key;
            if (dataSourceService.dataSourceExists(dsName)) {
              super.put(dsName, new DataSourceSubSchema(dataSourceService, dsName));
            }
          }
          return super.get(key);
        }
      };

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
    if (table instanceof org.apache.calcite.schema.Table calciteTable) {
      tableMap.put(qualifiedName.toString(), calciteTable);
    } else {
      throw new UnsupportedOperationException(
          "Table "
              + qualifiedName
              + " does not support Calcite integration. "
              + "The storage engine table must implement org.apache.calcite.schema.Table.");
    }
  }

  /**
   * A sub-schema representing a non-default datasource. Lazily resolves tables from the
   * datasource's storage engine, allowing Calcite to find tables via schema-qualified names like
   * scan(["prometheus", "up"]).
   */
  private static class DataSourceSubSchema extends AbstractSchema {
    private final DataSourceService dataSourceService;
    private final String dataSourceName;

    DataSourceSubSchema(DataSourceService dataSourceService, String dataSourceName) {
      this.dataSourceService = dataSourceService;
      this.dataSourceName = dataSourceName;
    }

    @Override
    protected Map<String, Table> getTableMap() {
      return tableMap;
    }

    private final Map<String, Table> tableMap =
        new HashMap<>() {
          @Override
          public Table get(Object key) {
            if (!super.containsKey(key)) {
              resolveTable((String) key);
            }
            return super.get(key);
          }
        };

    private void resolveTable(String tableName) {
      org.opensearch.sql.storage.Table table =
          dataSourceService
              .getDataSource(dataSourceName)
              .getStorageEngine()
              .getTable(new DataSourceSchemaName(dataSourceName, "default"), tableName);
      if (table instanceof org.apache.calcite.schema.Table calciteTable) {
        tableMap.put(tableName, calciteTable);
      } else {
        throw new UnsupportedOperationException(
            "Table "
                + dataSourceName
                + "."
                + tableName
                + " does not support Calcite integration. "
                + "The storage engine table must implement org.apache.calcite.schema.Table.");
      }
    }
  }
}
