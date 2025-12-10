/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * {@code UnifiedQueryPlanner} provides a high-level API for parsing and analyzing queries using the
 * Calcite-based query engine. It serves as the primary integration point for external consumers
 * such as Spark or command-line tools, abstracting away Calcite internals.
 */
public class UnifiedQueryPlanner {
  /** The parser instance responsible for converting query text into a parse tree. */
  private final Parser parser;

  /** Unified query context containing CalcitePlanContext with all configuration. */
  private final UnifiedQueryContext context;

  /** AST-to-RelNode visitor that builds logical plans from the parsed AST. */
  private final CalciteRelNodeVisitor relNodeVisitor =
      new CalciteRelNodeVisitor(new EmptyDataSourceService());

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.parser = buildQueryParser(context.getPlanContext().queryType);
    this.context = context;
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL or other supported syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      return preserveCollation(analyze(parse(query)));
    } catch (SyntaxCheckException e) {
      // Re-throw syntax error without wrapping
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query", e);
    }
  }

  private Parser buildQueryParser(QueryType queryType) {
    if (queryType == QueryType.PPL) {
      return new PPLSyntaxParser();
    }
    throw new IllegalArgumentException("Unsupported query type: " + queryType);
  }

  private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query, context.getSettings()),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  private RelNode analyze(UnresolvedPlan ast) {
    return relNodeVisitor.analyze(ast, context.getPlanContext());
  }

  private RelNode preserveCollation(RelNode logical) {
    RelNode calcitePlan = logical;
    RelCollation collation = logical.getTraitSet().getCollation();
    if (!(logical instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(logical, collation, null, null);
    }
    return calcitePlan;
  }
}
