/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest.analytics.stub;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Stub schema provider for development and testing. Returns hardcoded Calcite table definitions
 * with standard types. Will be replaced by {@code EngineContext.getSchema()} when the analytics
 * engine is ready.
 */
public class StubSchemaProvider {

  /** Build a stub Calcite schema with hardcoded parquet tables. */
  public static AbstractSchema buildSchema() {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of(
            "parquet_logs", buildLogsTable(),
            "parquet_metrics", buildMetricsTable());
      }
    };
  }

  private static Table buildLogsTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory
            .builder()
            .add("ts", SqlTypeName.TIMESTAMP)
            .add("status", SqlTypeName.INTEGER)
            .add("message", SqlTypeName.VARCHAR)
            .add("ip_addr", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }

  private static Table buildMetricsTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory
            .builder()
            .add("ts", SqlTypeName.TIMESTAMP)
            .add("cpu", SqlTypeName.DOUBLE)
            .add("memory", SqlTypeName.DOUBLE)
            .add("host", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }
}
