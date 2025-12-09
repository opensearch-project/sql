/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryPlannerTest extends UnifiedQueryTestBase {

  /** Test catalog consists of test schema above */
  private final AbstractSchema testDeepSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Schema> getSubSchemaMap() {
          return Map.of("opensearch", testSchema);
        }
      };

  @Test
  public void testPPLQueryPlanning() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan = planner.plan("source = opensearch.employees | eval f = abs(id)");
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespace() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    assertNotNull("Plan should be created", planner.plan("source = opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespaceMultiLevel() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("catalog", testDeepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    assertNotNull("Plan should be created", planner.plan("source = catalog.opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));

    // This is valid in SparkSQL, but Calcite requires "catalog" as the default root schema to
    // resolve it
    assertThrows(IllegalStateException.class, () -> planner.plan("source = opensearch.employees"));
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogs() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan =
        planner.plan(
            "source = catalog1.employees | lookup catalog2.employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogsAndDefaultNamespace() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .defaultNamespace("catalog2")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan =
        planner.plan("source = catalog1.employees | lookup employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMetadataCaching() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .cacheMetadata(true)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan = planner.plan("source = opensearch.employees");
    assertNotNull("Plan should be created", plan);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingQueryLanguage() {
    UnifiedQueryContext.builder().catalog("opensearch", testSchema).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedQueryLanguage() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.SQL) // only PPL is supported for now
            .catalog("opensearch", testSchema)
            .build();
    new UnifiedQueryPlanner(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDefaultNamespacePath() {
    UnifiedQueryContext.builder()
        .queryType(QueryType.PPL)
        .catalog("opensearch", testSchema)
        .defaultNamespace("nonexistent") // nonexistent namespace path
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testUnsupportedStatementType() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    planner.plan("explain source = employees"); // explain statement
  }

  @Test(expected = SyntaxCheckException.class)
  public void testPlanPropagatingSyntaxCheckException() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .queryType(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    planner.plan("source = employees | eval"); // Trigger syntax error from parser
  }
}
