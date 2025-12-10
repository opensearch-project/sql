/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.opensearch.sql.executor.QueryType;

/** Base class for unified query tests providing common test schema and utilities. */
public abstract class UnifiedQueryTestBase {

  /** Test schema containing sample tables for testing */
  protected AbstractSchema testSchema;

  /** Unified query planner configured with test schema */
  protected UnifiedQueryPlanner planner;

  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("employees", createEmployeesTable());
          }
        };

    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  protected Table createEmployeesTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            List.of("id", "name", "age", "department"));
      }
    };
  }
}
