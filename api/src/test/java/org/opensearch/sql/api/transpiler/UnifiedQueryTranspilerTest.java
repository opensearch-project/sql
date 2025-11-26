/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpiler;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryTranspilerTest extends UnifiedQueryTestBase {

  private UnifiedQueryPlanner planner;
  private UnifiedQueryTranspiler transpiler;

  @Before
  public void setUp() {
    super.setUp();
    planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .build();

    transpiler = UnifiedQueryTranspiler.builder().dialect(SparkSqlDialect.DEFAULT).build();
  }

  @Test
  public void testToSql() {
    String pplQuery = "source = employees";
    RelNode plan = planner.plan(pplQuery);

    String sql = transpiler.toSql(plan);
    String expectedSql = "SELECT *\nFROM `catalog`.`employees`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }
}
