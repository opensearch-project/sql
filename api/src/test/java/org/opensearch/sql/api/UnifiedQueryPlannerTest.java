/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryPlannerTest {

  /** Test database with a test table with id and name columns */
  private AbstractSchema testDatabase =
      new AbstractSchema() {
        @Override
        protected Map<String, Table> getTableMap() {
          return Map.of(
              "test",
              new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                  final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
                  final RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                  return typeFactory.createStructType(
                      List.of(intType, stringType), List.of("id", "name"));
                }
              });
        }
      };

  @Test
  public void testSimpleQuery() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", Map.of("default", testDatabase))
            .build();

    RelNode plan = planner.plan("source = opensearch.default.test | eval f = abs(123)");
    assertNotNull("Plan should not be null", plan);
  }

  @Test
  public void testJoinQuery() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", Map.of("default", testDatabase))
            .catalog("spark_catalog", Map.of("default", testDatabase))
            .build();

    RelNode plan =
        planner.plan(
            "source = opensearch.default.test |"
                + "lookup spark_catalog.default.test id |"
                + "eval f = abs(123)");
    assertNotNull("Plan should not be null", plan);
  }
}
