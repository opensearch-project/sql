/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for library functions registered in {@link UnifiedFunctionSpec#LIBRARY}. Verifies both
 * planning (function resolves correctly) and execution (produces correct results in-memory).
 */
public class UnifiedFunctionLibraryTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  @Test
  public void testLengthPlanning() {
    givenQuery("SELECT LENGTH(name) AS len FROM catalog.employees")
        .assertPlanContains("LENGTH($1)");
  }

  @Test
  public void testLengthExecution() throws Exception {
    RelNode plan = planner.plan("SELECT LENGTH(name) AS len FROM catalog.employees");
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("len", INTEGER))
          .expectData(row(5), row(3), row(7), row(5)); // Alice, Bob, Charlie, Diana
    }
  }

  @Test
  public void testRegexpReplacePlanning() {
    givenQuery("SELECT REGEXP_REPLACE(name, 'A.*', 'X') AS replaced FROM catalog.employees")
        .assertPlanContains("REGEXP_REPLACE($1, 'A.*', 'X')");
  }

  @Test
  public void testRegexpReplaceExecution() throws Exception {
    RelNode plan =
        planner.plan("SELECT REGEXP_REPLACE(name, '^A.*', 'Replaced') FROM catalog.employees");
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("EXPR$0", VARCHAR))
          .expectData(row("Replaced"), row("Bob"), row("Charlie"), row("Diana"));
    }
  }

  @Test
  public void testDateTruncPlanning() {
    // Plan preserves DATE_TRUNC (late binding — not rewritten until compilation)
    givenQuery(
            "SELECT DATE_TRUNC('minute', TIMESTAMP '2023-01-01 12:34:56') AS truncated"
                + " FROM catalog.employees")
        .assertPlanContains("DATE_TRUNC('minute'");
  }

  @Test
  public void testFunctionSpecLookup() {
    assertTrue(UnifiedFunctionSpec.of("length").isPresent());
    assertTrue(UnifiedFunctionSpec.of("regexp_replace").isPresent());
    assertTrue(UnifiedFunctionSpec.of("date_trunc").isPresent());
  }
}
