/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.compiler;

import static java.sql.Types.BIGINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryTestBase;

public class UnifiedQueryCompilerTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    RelNode plan = planner.plan("source = catalog.employees | where age > 30");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(
              col("id", INTEGER),
              col("name", VARCHAR),
              col("age", INTEGER),
              col("department", VARCHAR))
          .expectData(row(2, "Bob", 35, "Sales"), row(3, "Charlie", 45, "Engineering"));
    }
  }

  @Test
  public void testComplexQuery() throws Exception {
    RelNode plan = planner.plan("source = catalog.employees | stats count() by department");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(col("count()", BIGINT), col("department", VARCHAR))
          .expectData(row(2L, "Engineering"), row(1L, "Sales"), row(1L, "Marketing"));
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCompileFailure() {
    RelNode mockPlan = Mockito.mock(RelNode.class);
    Mockito.when(mockPlan.accept(Mockito.any(RelShuttle.class)))
        .thenThrow(new RuntimeException("Intentional compilation failure"));

    compiler.compile(mockPlan);
  }
}
