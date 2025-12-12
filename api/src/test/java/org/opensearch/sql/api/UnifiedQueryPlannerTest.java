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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;

@RunWith(MockitoJUnitRunner.class)
public class UnifiedQueryPlannerTest extends UnifiedQueryTestBase {

  /** Test catalog consists of test schema above */
  private final AbstractSchema testDeepSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Schema> getSubSchemaMap() {
          return Map.of("opensearch", testSchema);
        }
      };

  @Mock private Settings testSettings;

  @Test
  public void testPPLQueryPlanning() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();

    RelNode plan = planner.plan("source = opensearch.employees | eval f = abs(id)");
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespace() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build();

    assertNotNull("Plan should be created", planner.plan("source = opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespaceMultiLevel() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testDeepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();

    assertNotNull("Plan should be created", planner.plan("source = catalog.opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));

    // This is valid in SparkSQL, but Calcite requires "catalog" as the default root schema to
    // resolve it
    assertThrows(IllegalStateException.class, () -> planner.plan("source = opensearch.employees"));
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
        planner.plan(
            "source = catalog1.employees | lookup catalog2.employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogsAndDefaultNamespace() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .defaultNamespace("catalog2")
            .build();

    RelNode plan =
        planner.plan("source = catalog1.employees | lookup employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMetadataCaching() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .cacheMetadata(true)
            .build();

    RelNode plan = planner.plan("source = opensearch.employees");
    assertNotNull("Plan should be created", plan);
  }

  @Test(expected = NullPointerException.class)
  public void testMissingQueryLanguage() {
    UnifiedQueryPlanner.builder().catalog("opensearch", testSchema).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnsupportedQueryLanguage() {
    UnifiedQueryPlanner.builder()
        .language(QueryType.SQL) // only PPL is supported for now
        .catalog("opensearch", testSchema)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDefaultNamespacePath() {
    UnifiedQueryPlanner.builder()
        .language(QueryType.PPL)
        .catalog("opensearch", testSchema)
        .defaultNamespace("nonexistent") // nonexistent namespace path
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testUnsupportedStatementType() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();

    planner.plan("explain source = employees"); // explain statement
  }

  @Test(expected = SyntaxCheckException.class)
  public void testPlanPropagatingSyntaxCheckException() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .build();

    planner.plan("source = employees | eval"); // Trigger syntax error from parser
  }

  @Test
  public void testJoinQuery() {
    UnifiedQueryPlanner planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .settings(testSettings)
            .build();

    planner.plan("source = employees | join on employees.id = payslips.id payslips");
  }
}
