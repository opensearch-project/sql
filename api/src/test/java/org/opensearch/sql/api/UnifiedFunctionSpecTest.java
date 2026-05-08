/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.api.spec.UnifiedFunctionSpec;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for scalar functions registered in {@link UnifiedFunctionSpec#SCALAR}. Verifies planning
 * (function resolves correctly) and execution (produces correct results in-memory).
 */
public class UnifiedFunctionSpecTest extends UnifiedQueryTestBase {

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
  public void testLength() throws Exception {
    assertEquals(5, eval("LENGTH('hello')"));
    assertEquals(0, eval("LENGTH('')"));
  }

  @Test
  public void testRegexpReplace() throws Exception {
    assertEquals("XbcXbc", eval("REGEXP_REPLACE('abcabc', 'a', 'X')"));
    assertEquals("hello", eval("REGEXP_REPLACE('hello', 'xyz', 'X')"));
  }

  @Test
  public void testDateTrunc() throws Exception {
    // Plan preserves DATE_TRUNC (late binding — not rewritten until compilation)
    givenQuery(
            "SELECT DATE_TRUNC('minute', TIMESTAMP '2023-01-01 12:34:56') FROM catalog.employees")
        .assertPlanContains("DATE_TRUNC('minute', 2023-01-01 12:34:56)");

    // Execution rewrites to FLOOR and produces truncated timestamp
    Object result = eval("DATE_TRUNC('hour', TIMESTAMP '2023-07-15 14:30:45')");
    assertEquals(Timestamp.valueOf("2023-07-15 14:00:00"), result);
  }

  @Test
  public void testFunctionSpecLookup() {
    assertTrue(UnifiedFunctionSpec.of("length").isPresent());
    assertTrue(UnifiedFunctionSpec.of("regexp_replace").isPresent());
    assertTrue(UnifiedFunctionSpec.of("date_trunc").isPresent());
  }

  private Object eval(String expr) throws Exception {
    RelNode plan = planner.plan("SELECT " + expr + " AS v FROM (VALUES (0)) AS t(dummy)");
    try (PreparedStatement stmt = compiler.compile(plan);
        ResultSet rs = stmt.executeQuery()) {
      assertTrue(rs.next());
      return rs.getObject(1);
    }
  }
}
