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

  /** Test schema consists of a test table with id and name columns */
  private final AbstractSchema testSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Table> getTableMap() {
          return Map.of(
              "test",
              new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                  return typeFactory.createStructType(
                      List.of(
                          typeFactory.createSqlType(SqlTypeName.INTEGER),
                          typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                      List.of("id", "name"));
                }
              });
        }
      };

  @Test
  public void testPPLQueryPlanning() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();

    RelNode plan = planner.plan("source = opensearch.test | eval f = abs(id)");
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogs() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .build();

    RelNode plan =
        planner.plan("source = catalog1.test | lookup catalog2.test id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithSchemaCaching() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .cacheSchema(true)
            .build();

    RelNode plan = planner.plan("source = opensearch.test | eval f = abs(id)");
    assertNotNull("Planner should work with schema caching enabled", plan);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingQueryLanguage() {
    UnifiedQueryPlanner.builder().catalog("opensearch", testSchema).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedQueryLanguage() {
    UnifiedQueryPlanner.builder()
        .language(QueryType.SQL) // only PPL is supported
        .catalog("opensearch", testSchema)
        .build();
  }
}
