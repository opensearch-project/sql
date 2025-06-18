/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
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
  private AbstractSchema testSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Table> getTableMap() {
          return new HashMap<>() {
            @Override
            public Table get(Object key) {
              return new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                  final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
                  final RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
                  return typeFactory.createStructType(
                      List.of(intType, stringType), List.of("id", "name"));
                }
              };
            }
          };
        }
      };

  @Test
  public void testSimpleQuery() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();

    RelNode plan = planner.plan("source = opensearch.test | eval f = abs(id)");
    assertNotNull("Plan should not be null", plan);
  }

  @Test
  public void testJoinQuery() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .catalog("spark_catalog", testSchema)
            .build();

    RelNode plan =
        planner.plan(
            "source = opensearch.test |" + "lookup spark_catalog.test id |" + "eval f = abs(id)");
    assertNotNull("Plan should not be null", plan);
  }
}
