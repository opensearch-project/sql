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
import org.opensearch.sql.calcite.validate.OpenSearchSparkSqlDialect;

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

    String actualSql = transpiler.toSql(plan);
    String expectedSql = normalize("SELECT *\nFROM `catalog`.`employees`");
    assertEquals(
        "Transpiled SQL using SparkSqlDialect should match expected SQL", expectedSql, actualSql);
  }

  @Test
  public void testToSqlWithCustomDialect() {
    String pplQuery = "source = employees | where name = 123";
    RelNode plan = planner.plan(pplQuery);

    UnifiedQueryTranspiler customTranspiler =
        UnifiedQueryTranspiler.builder().dialect(OpenSearchSparkSqlDialect.DEFAULT).build();
    String actualSql = customTranspiler.toSql(plan);
    String expectedSql =
        normalize(
            "SELECT *\nFROM `catalog`.`employees`\nWHERE TRY_CAST(`name` AS DOUBLE) = 1.230E2");
    assertEquals(
        "Transpiled query using OpenSearchSparkSqlDialect should translate SAFE_CAST to TRY_CAST",
        expectedSql,
        actualSql);
  }

  /** Normalizes line endings to platform-specific format for cross-platform test compatibility. */
  private String normalize(String sql) {
    return sql.replace("\n", System.lineSeparator());
  }
}
