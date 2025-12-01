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
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect;

public class UnifiedQueryTranspilerTest extends UnifiedQueryTestBase {

  private UnifiedQueryTranspiler transpiler;

  @Before
  public void setUp() {
    super.setUp();
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

  @Test
  public void testToSqlWithCustomDialect() {
    String pplQuery = "source = employees | where name = 123";
    RelNode plan = planner.plan(pplQuery);

    // OpenSearchSparkSqlDialect translates SAFE_CAST to TRY_CAST for Spark SQL compatibility
    UnifiedQueryTranspiler customTranspiler =
        UnifiedQueryTranspiler.builder().dialect(OpenSearchSparkSqlDialect.DEFAULT).build();
    String sql = customTranspiler.toSql(plan);
    String expectedCustomSql =
        "SELECT *\n"
            + "FROM `catalog`.`employees`\n"
            + "WHERE TRY_CAST(`name` AS DOUBLE) = 1.230E2";
    assertEquals(
        "OpenSearchSparkSqlDialect should translate SAFE_CAST to TRY_CAST", expectedCustomSql, sql);
  }
}
