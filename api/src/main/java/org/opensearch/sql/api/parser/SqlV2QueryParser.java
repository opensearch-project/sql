/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.existsSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.inSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.join;
import static org.opensearch.sql.ast.dsl.AstDSL.union;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Join.JoinType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ExistsSubqueryExpressionAtomContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.InSubqueryPredicateContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.JoinClauseContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.UnionSelectContext;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/** SQL query parser that produces {@link UnresolvedPlan} using the V2 ANTLR grammar. */
public class SqlV2QueryParser implements UnifiedQueryParser<UnresolvedPlan> {

  /** Reusable ANTLR-based SQL syntax parser. Stateless and thread-safe. */
  private final SQLSyntaxParser syntaxParser = new SQLSyntaxParser();

  @Override
  public UnresolvedPlan parse(String query) {
    ParseTree cst = syntaxParser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new ExtendedAstBuilder(query),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  /**
   * Extends the V2 AstBuilder with JOIN support that the base AstBuilder rejects with
   * SyntaxCheckException to trigger legacy engine fallback.
   */
  private static class ExtendedAstBuilder extends AstBuilder {

    ExtendedAstBuilder(String query) {
      super(query);
    }

    @Override
    protected AstExpressionBuilder createExpressionBuilder() {
      return new ExtendedAstExpressionBuilder();
    }

    @Override
    public UnresolvedPlan visitJoinClause(JoinClauseContext ctx) {
      JoinType joinType = toJoinType(ctx);
      UnresolvedPlan right = visit(ctx.relation());
      Optional<UnresolvedExpression> condition =
          Optional.ofNullable(ctx.expression()).map(this::visitAstExpression);
      return join(right, joinType, condition);
    }

    private JoinType toJoinType(JoinClauseContext ctx) {
      return switch (ctx.getStart().getType()) {
        case OpenSearchSQLParser.LEFT -> JoinType.LEFT;
        case OpenSearchSQLParser.RIGHT -> JoinType.RIGHT;
        case OpenSearchSQLParser.CROSS -> JoinType.CROSS;
        default -> JoinType.INNER;
      };
    }

    @Override
    public UnresolvedPlan visitUnionSelect(UnionSelectContext ctx) {
      List<UnresolvedPlan> datasets =
          ctx.querySpecification().stream().map(this::visit).collect(Collectors.toList());
      return union(datasets);
    }

    /**
     * Expression builder with IN/EXISTS subquery support. Accesses the enclosing AstBuilder to
     * visit subquery plan nodes. Must be created via {@link #createExpressionBuilder()} because the
     * enclosing {@code this} reference is not available during {@code super()} construction.
     */
    private class ExtendedAstExpressionBuilder extends AstExpressionBuilder {

      @Override
      public UnresolvedExpression visitInSubqueryPredicate(InSubqueryPredicateContext ctx) {
        UnresolvedPlan subquery = ExtendedAstBuilder.this.visit(ctx.querySpecification());
        UnresolvedExpression inExpr = inSubquery(subquery, visit(ctx.predicate()));
        return (ctx.NOT() != null) ? new Not(inExpr) : inExpr;
      }

      @Override
      public UnresolvedExpression visitExistsSubqueryExpressionAtom(
          ExistsSubqueryExpressionAtomContext ctx) {
        UnresolvedPlan subquery = ExtendedAstBuilder.this.visit(ctx.querySpecification());
        return existsSubquery(subquery);
      }
    }
  }
}
