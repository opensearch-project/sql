/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.monitor.profile.MetricName.ANALYZE;

import java.util.List;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.api.parser.UnifiedQueryParser;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * {@code UnifiedQueryPlanner} provides a high-level API for parsing and analyzing queries using the
 * Calcite-based query engine. It serves as the primary integration point for external consumers
 * such as Spark or command-line tools, abstracting away Calcite internals.
 */
public class UnifiedQueryPlanner {

  private static final Logger LOG = LogManager.getLogger(UnifiedQueryPlanner.class);

  /** The statement check of a planner that has none. */
  private static final Consumer<UnresolvedPlan> NO_CHECK = statement -> {};

  /** Planning strategy selected at construction time based on query type. */
  private final PlanningStrategy strategy;

  /** Called with the AST of each statement before it is lowered. */
  private final Consumer<UnresolvedPlan> statementCheck;

  /** Unified query context for profiling support. */
  private final UnifiedQueryContext context;

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this(context, NO_CHECK);
  }

  /**
   * Constructs a UnifiedQueryPlanner that also checks every parsed statement before lowering it,
   * e.g. to reject what the target engine cannot run. The check runs inside {@link #plan(String)},
   * so its errors are translated and profiled like planning errors, and it runs whatever the
   * planning strategy.
   *
   * @param context the unified query context containing CalcitePlanContext
   * @param statementCheck called with the AST of each statement; throws to reject it
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context, Consumer<UnresolvedPlan> statementCheck) {
    this.context = context;
    this.statementCheck = statementCheck;
    this.strategy = new CustomVisitorStrategy(context);
    if (statementCheck != NO_CHECK && !strategy.appliesStatementChecks()) {
      throw new IllegalArgumentException(
          "This planning strategy has no plugin AST to apply a statement check to");
    }
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL or SQL syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      return context.measure(
          ANALYZE,
          () -> {
            RelNode plan = strategy.plan(query, statementCheck);
            for (var shuttle : context.getLangSpec().postAnalysisRules()) {
              plan = plan.accept(shuttle);
            }
            return plan;
          });
    } catch (CalciteUnsupportedException e) {
      // Unsupported feature (e.g. table functions) is an invalid query, i.e. a client error.
      // Must precede the QueryEngineException branch as it is a subclass.
      throw new SemanticCheckException(e.getMessage(), e);
    } catch (SyntaxCheckException
        | QueryEngineException
        | UnsupportedOperationException
        | IllegalArgumentException
        | ErrorReport e) {
      LOG.warn("Failed to plan query: {}", e.getMessage());
      throw e;
    } catch (CalciteException e) {
      // Calcite validation errors (e.g. table not found) indicate an invalid query.
      LOG.warn("Failed to plan query, invalid query: {}", e.getMessage());
      throw new SemanticCheckException(e.getMessage(), e);
    } catch (AssertionError e) {
      // Calcite throws assertion error directly when building bad RelNode
      String message = "Failed to plan query: invalid plan structure";
      LOG.warn(message, e);
      throw new SemanticCheckException(message, e);
    } catch (Exception e) {
      String message = "Failed to plan query: unexpected error";
      LOG.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  /** Strategy interface for language-specific planning logic. */
  private interface PlanningStrategy {
    /**
     * Plan {@code query}; if {@link #appliesStatementChecks()}, {@code statementCheck} sees the
     * statement's AST before it is lowered.
     */
    RelNode plan(String query, Consumer<UnresolvedPlan> statementCheck) throws Exception;

    /** Whether the strategy plans from the plugin's AST, so it can apply a statement check. */
    boolean appliesStatementChecks();
  }

  /** ANSI SQL planning using Calcite's native SqlParser → SqlValidator → SqlToRelConverter. */
  @RequiredArgsConstructor
  private static class CalciteNativeStrategy implements PlanningStrategy {
    private final UnifiedQueryContext context;

    /**
     * No: Calcite parses the query with its own SQL parser, so there is no plugin AST, and the
     * planner refuses a statement check with this strategy. Nothing a check rejects today slips
     * through: {@code knn} is not a Calcite SQL function, so a k-NN query fails validation here.
     */
    @Override
    public boolean appliesStatementChecks() {
      return false;
    }

    @Override
    public RelNode plan(String query, Consumer<UnresolvedPlan> statementCheck) throws Exception {
      try (Planner planner = Frameworks.getPlanner(context.getPlanContext().config)) {
        SqlNode parsed = planner.parse(query);
        if (!parsed.isA(SqlKind.QUERY)) {
          throw new UnsupportedOperationException(
              "Only query statements are supported. Got: " + parsed.getKind());
        }

        // TODO: move post-parse rewriting into CalciteSqlQueryParser
        SqlNode rewritten = parsed;
        for (SqlVisitor<SqlNode> visitor : context.getLangSpec().postParseRules()) {
          rewritten = rewritten.accept(visitor);
        }

        SqlNode validated = planner.validate(rewritten);
        RelRoot relRoot = planner.rel(validated);
        return relRoot.project();
      }
    }
  }

  /** AST-based planning via context-owned parser → UnresolvedPlan → CalciteRelNodeVisitor. */
  private static class CustomVisitorStrategy implements PlanningStrategy {
    private final UnifiedQueryContext context;
    private final UnifiedQueryParser<UnresolvedPlan> parser;
    private final CalciteRelNodeVisitor relNodeVisitor =
        new CalciteRelNodeVisitor(new EmptyDataSourceService());

    @SuppressWarnings("unchecked")
    CustomVisitorStrategy(UnifiedQueryContext context) {
      this.context = context;
      this.parser = (UnifiedQueryParser<UnresolvedPlan>) context.getParser();
    }

    @Override
    public boolean appliesStatementChecks() {
      return true;
    }

    @Override
    public RelNode plan(String query, Consumer<UnresolvedPlan> statementCheck) {
      UnresolvedPlan ast = parser.parse(query);
      statementCheck.accept(ast);
      RelNode logical = relNodeVisitor.analyzeStatement(ast, context.getPlanContext());
      return preserveCollation(logical);
    }

    private RelNode preserveCollation(RelNode logical) {
      if (logical instanceof Sort) {
        return logical;
      }
      List<RelCollation> collations =
          logical.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
      if (collations != null
          && collations.size() == 1
          && !collations.get(0).getFieldCollations().isEmpty()) {
        return LogicalSort.create(logical, collations.get(0), null, null);
      }
      return logical;
    }
  }
}
