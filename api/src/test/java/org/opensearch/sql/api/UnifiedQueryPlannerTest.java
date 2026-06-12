/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.Test;
import org.opensearch.sql.api.spec.LanguageSpec;
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
        .assertErrorMessageContains("Unexpected window function: rank");
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

  /**
   * A post-analysis rule that raises {@link AssertionError} — as Calcite does when it builds an
   * inconsistent RelNode — must be normalized to a {@link SemanticCheckException} so callers
   * classify it as a 4xx rather than an unhandled 500.
   */
  @Test
  public void assertionErrorInPostAnalysisRuleIsWrappedAsSemanticCheckException() {
    UnifiedQueryPlanner localPlanner = plannerWithPostAnalysisRule(new AssertionErrorShuttle());

    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class, () -> localPlanner.plan("source = catalog.employees"));
    assertEquals("Failed to plan query: invalid plan structure", ex.getMessage());
    assertTrue(
        "Expected AssertionError cause, got " + ex.getCause(),
        ex.getCause() instanceof AssertionError);
  }

  /**
   * An unexpected {@link RuntimeException} in a post-analysis rule falls through to the generic
   * catch and is wrapped as an {@link IllegalStateException}, keeping the surface stable for
   * callers even when a rule misbehaves.
   */
  @Test
  public void unexpectedExceptionInPostAnalysisRuleIsWrappedAsIllegalStateException() {
    UnifiedQueryPlanner localPlanner = plannerWithPostAnalysisRule(new RuntimeExceptionShuttle());

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> localPlanner.plan("source = catalog.employees"));
    assertEquals("Failed to plan query: unexpected error", ex.getMessage());
    assertTrue(
        "Expected RuntimeException cause, got " + ex.getCause(),
        ex.getCause() instanceof RuntimeException);
  }

  /**
   * Builds a planner whose language spec wraps the current context's spec with an additional
   * post-analysis rule. Used by the catch-branch tests above.
   */
  private UnifiedQueryPlanner plannerWithPostAnalysisRule(RelShuttle rule) {
    LanguageSpec baseSpec = context.getLangSpec();
    LanguageSpec throwingSpec =
        new LanguageSpec() {
          @Override
          public SqlParser.Config parserConfig() {
            return baseSpec.parserConfig();
          }

          @Override
          public SqlValidator.Config validatorConfig() {
            return baseSpec.validatorConfig();
          }

          @Override
          public List<LanguageExtension> extensions() {
            return baseSpec.extensions();
          }

          @Override
          public List<RelShuttle> postAnalysisRules() {
            return List.of(rule);
          }
        };
    UnifiedQueryContext throwingContext =
        new UnifiedQueryContext(
            context.getPlanContext(), context.getSettings(), context.getParser(), throwingSpec);
    return new UnifiedQueryPlanner(throwingContext);
  }

  /** RelShuttle that always throws {@link AssertionError} on the first visited node. */
  private static class AssertionErrorShuttle extends RelShuttleImpl {
    @Override
    public RelNode visit(TableScan scan) {
      throw new AssertionError("simulated bad RelNode");
    }

    @Override
    public RelNode visit(RelNode other) {
      throw new AssertionError("simulated bad RelNode");
    }
  }

  /** RelShuttle that always throws {@link RuntimeException} on the first visited node. */
  private static class RuntimeExceptionShuttle extends RelShuttleImpl {
    @Override
    public RelNode visit(TableScan scan) {
      throw new RuntimeException("simulated post-analysis failure");
    }

    @Override
    public RelNode visit(RelNode other) {
      throw new RuntimeException("simulated post-analysis failure");
    }
  }
}
