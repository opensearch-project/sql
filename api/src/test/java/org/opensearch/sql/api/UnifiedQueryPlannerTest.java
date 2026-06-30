/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.SemanticCheckException;
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
    RelNode plan = planner.plan("source = catalog.employees | eval f = abs(id)");
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testPPLJoinQueryPlanning() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | join left = l right = r on l.id = r.age"
                + " catalog.employees");
    assertNotNull("Join query should be created", plan);
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespace() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
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
            .language(QueryType.PPL)
            .catalog("catalog", testDeepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    assertNotNull("Plan should be created", planner.plan("source = catalog.opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));

    // This is valid in SparkSQL, but Calcite requires "catalog" as the default root schema to
    // resolve it
    assertThrows(SemanticCheckException.class, () -> planner.plan("source = opensearch.employees"));
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogs() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
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
            .language(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .defaultNamespace("catalog2")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan =
        planner.plan("source = catalog1.employees | lookup employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedStatementType() {
    planner.plan("explain source = catalog.employees"); // explain statement
  }

  @Test(expected = SyntaxCheckException.class)
  public void testPlanPropagatingSyntaxCheckException() {
    planner.plan("source = catalog.employees | eval"); // Trigger syntax error from parser
  }

  @Test
  public void syntaxErrorIsRethrownAsSyntaxCheckException() {
    givenInvalidQuery("INVALID +++")
        .assertErrorType(SyntaxCheckException.class)
        .assertErrorMessageContains("is not a valid term");
  }

  @Test
  public void semanticErrorIsRethrownAsSemanticCheckException() {
    givenInvalidQuery("source = catalog.employees | rename id* as x*y*")
        .assertErrorType(SemanticCheckException.class)
        .assertErrorMessageEquals("Source and target patterns have different wildcard counts");
  }

  @Test
  public void fieldNotFoundIsRethrownAsErrorReport() {
    givenInvalidQuery("source = catalog.employees | where unknown_field = 1")
        .assertErrorType(ErrorReport.class)
        .assertErrorMessageContains("Field [unknown_field] not found");
  }

  @Test
  public void invalidTableIsRethrownAsSemanticCheckException() {
    givenInvalidQuery("source = catalog.nonexistent_table")
        .assertErrorType(SemanticCheckException.class)
        .assertCauseType(CalciteException.class);
  }

  @Test
  public void unsupportedFeatureIsRethrownAsSemanticCheckException() {
    // A feature unsupported on the analytics engine (here a PPL command that raises
    // CalciteUnsupportedException; SQL table functions like vectorSearch() take the same path) is
    // an invalid query, normalized to a SemanticCheckException so callers classify it as a 4xx.
    givenInvalidQuery("source = catalog.employees | kmeans")
        .assertErrorType(SemanticCheckException.class)
        .assertCauseType(CalciteUnsupportedException.class)
        .assertErrorMessageContains("unsupported in Calcite");
  }

  @Test
  public void unsupportedWindowFunctionIsRethrownAsSemanticCheckException() {
    // Window functions outside WINDOW_FUNC_MAPPING reach
    // CalciteRexNodeVisitor#visitWindowFunction's
    // orElseThrow. The throw site emits CalciteUnsupportedException so this path normalizes to a
    // 4xx SemanticCheckException rather than escaping as a 500.
    givenInvalidQuery("source = catalog.employees | eventstats rank()")
        .assertErrorType(SemanticCheckException.class)
        .assertCauseType(CalciteUnsupportedException.class)
        .assertErrorMessageContains("Window function 'rank' is not supported in eventstats/streamstats");
  }

  @Test
  public void assertionErrorIsWrappedAsSemanticCheckException() {
    // Remove when the underlying Calcite assertion is fixed.
    givenInvalidQuery(
            """
            source = catalog.employees
            | eval ts = timestamp('2024-01-01')
            | stats max(ts)
            """)
        .assertErrorType(SemanticCheckException.class)
        .assertErrorMessageEquals("Failed to plan query: invalid plan structure")
        .assertCauseType(AssertionError.class);
  }

  /**
   * Without the {@code PATTERN_*} defaults in {@link UnifiedQueryContext}, a bare {@code patterns
   * <field>} (no explicit {@code method=}/{@code mode=}) dies at parse time with {@code
   * PatternMethod.valueOf("NULL")} because {@code AstBuilder.visitPatternsCommand} reads a null
   * from {@code settings.getSettingValue(Key.PATTERN_METHOD)}. With the defaults present, the
   * planner lowers patterns to SIMPLE / LABEL mode and adds {@code patterns_field}.
   */
  @Test
  public void testPPLPatternsPicksUpDefaults() {
    givenQuery("source = catalog.employees | patterns name")
        .assertPlanContains("REGEXP_REPLACE")
        .assertFields("id", "name", "age", "department", "patterns_field");
  }
}
