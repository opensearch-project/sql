/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.TimeBounds;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.SupportsIndexPruning;

@Getter
public class OpenSearchSchema extends AbstractSchema {
  public static final String OPEN_SEARCH_SCHEMA_NAME = "OpenSearch";

  private final DataSourceService dataSourceService;

  /** Bounds every table resolved here is narrowed to, or null when the request declared none. */
  @Nullable private final TimeBounds timeBounds;

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

  public OpenSearchSchema(DataSourceService dataSourceService) {
    this(dataSourceService, null);
  }

  public OpenSearchSchema(DataSourceService dataSourceService, @Nullable TimeBounds timeBounds) {
    this.dataSourceService = dataSourceService;
    this.timeBounds = timeBounds;
  }

  public void registerTable(QualifiedName qualifiedName) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(dataSourceService, qualifiedName.getParts());
    DataSourceSchemaName schemaName =
        new DataSourceSchemaName(nameResolver.getDataSourceName(), nameResolver.getSchemaName());
    StorageEngine engine =
        dataSourceService.getDataSource(nameResolver.getDataSourceName()).getStorageEngine();

    org.opensearch.sql.storage.Table table = resolve(engine, schemaName, nameResolver);
    tableMap.put(qualifiedName.toString(), (org.apache.calcite.schema.Table) table);
  }

  /**
   * Applied to every table the query resolves, a subsearch's source included -- the request-level
   * scope Splunk's time range picker and ES|QL's request {@code filter} have.
   */
  private org.opensearch.sql.storage.Table resolve(
      StorageEngine engine,
      DataSourceSchemaName schemaName,
      DataSourceSchemaIdentifierNameResolver nameResolver) {
    if (timeBounds != null && engine instanceof SupportsIndexPruning pruning) {
      return pruning.getTable(schemaName, nameResolver.getIdentifierName(), timeBounds);
    }
    return engine.getTable(schemaName, nameResolver.getIdentifierName());
  }
}
