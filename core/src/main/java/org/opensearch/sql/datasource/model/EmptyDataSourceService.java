package org.opensearch.sql.datasource.model;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

public class EmptyDataSourceService {
  private static DataSourceService emptyDataSourceService =
      new DataSourceService() {
        @Override
        public DataSource getDataSource(String dataSourceName) {
          return new DataSource(
              DEFAULT_DATASOURCE_NAME, DataSourceType.OPENSEARCH, storageEngine());
        }

        @Override
        public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
          return Set.of();
        }

        @Override
        public DataSourceMetadata getDataSourceMetadata(String name) {
          return null;
        }

        @Override
        public void createDataSource(DataSourceMetadata metadata) {}

        @Override
        public void updateDataSource(DataSourceMetadata dataSourceMetadata) {}

        @Override
        public void deleteDataSource(String dataSourceName) {}

        @Override
        public Boolean dataSourceExists(String dataSourceName) {
          return null;
        }
      };

  private static StorageEngine storageEngine() {
    Table table =
        new Table() {
          @Override
          public boolean exists() {
            return true;
          }

          @Override
          public void create(Map<String, ExprType> schema) {
            throw new UnsupportedOperationException("Create table is not supported");
          }

          @Override
          public Map<String, ExprType> getFieldTypes() {
            return null;
          }

          @Override
          public PhysicalPlan implement(LogicalPlan plan) {
            throw new UnsupportedOperationException();
          }

          public Map<String, ExprType> getReservedFieldTypes() {
            return ImmutableMap.of("_test", STRING);
          }
        };
    return (dataSourceSchemaName, tableName) -> table;
  }

  public static DataSourceService getEmptyDataSourceService() {
    return emptyDataSourceService;
  }
}
