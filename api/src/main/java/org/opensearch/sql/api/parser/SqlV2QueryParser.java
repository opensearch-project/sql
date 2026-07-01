/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.existsSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.inSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.join;
import static org.opensearch.sql.ast.dsl.AstDSL.union;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Join.JoinType;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.antlr.AstBuildGuard;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ExistsSubqueryExpressionAtomContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FromClauseContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.InSubqueryPredicateContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.JoinClauseContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrderByClauseContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QuerySpecificationContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.UnionSelectContext;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;
import org.opensearch.sql.sql.parser.AstSortBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;
import org.opensearch.sql.sql.parser.context.QuerySpecification;

/** SQL query parser that produces {@link UnresolvedPlan} using the V2 ANTLR grammar. */
@RequiredArgsConstructor
public class SqlV2QueryParser implements UnifiedQueryParser<UnresolvedPlan> {

  /** Settings containing execution limits and feature flags used by AST builders. */
  private final Settings settings;

  /** Reusable ANTLR-based SQL syntax parser. Stateless and thread-safe. */
  private final SQLSyntaxParser syntaxParser = new SQLSyntaxParser();

  @Override
  public UnresolvedPlan parse(String query) {
    ParseTree cst = syntaxParser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new ExtendedAstBuilder(query, settings),
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

    ExtendedAstBuilder(String query, Settings settings) {
      super(query, settings);
    }

    @Override
    public UnresolvedPlan visitQuerySpecification(QuerySpecificationContext queryContext) {
      if (!hasWindowFunctionInProjectList(queryContext)) {
        return super.visitQuerySpecification(queryContext);
      }

      context.push();
      context.peek().collect(queryContext, query);
      Project project = (Project) visit(queryContext.selectClause());
      UnresolvedPlan result = project.attach(visit(queryContext.fromClause()));

      // Window output must be computed before ORDER BY/LIMIT, so build Limit(Sort(Project(from)))
      OrderByClauseContext orderByClause = queryContext.fromClause().orderByClause();
      if (orderByClause != null) {
        result = new ExtendedAstSortBuilder(context.peek()).visit(orderByClause).attach(result);
      }
      if (queryContext.limitClause() != null) {
        result = visit(queryContext.limitClause()).attach(result);
      }

      context.pop();
      return result;
    }

    @Override
    public UnresolvedPlan visitFromClause(FromClauseContext ctx) {
      UnresolvedPlan from = super.visitFromClause(ctx);
      if (hasWindowFunctionInProjectList(context.peek()) && from instanceof Sort sort) {
        // Drop the ORDER BY Sort for window queries; it is re-attached above the Project
        return sort.getChild().get(0);
      }
      return from;
    }

    private boolean hasWindowFunctionInProjectList(QuerySpecificationContext queryContext) {
      if (queryContext.fromClause() == null) {
        return false;
      }
      QuerySpecification probe = new QuerySpecification();
      probe.collect(queryContext, query);
      return hasWindowFunctionInProjectList(probe);
    }

    private static boolean hasWindowFunctionInProjectList(QuerySpecification querySpec) {
      return querySpec.getSelectItems().stream().anyMatch(item -> item instanceof WindowFunction);
    }

    @Override
    protected AstExpressionBuilder createExpressionBuilder() {
      return new ExtendedAstExpressionBuilder(guard);
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

      ExtendedAstExpressionBuilder(AstBuildGuard guard) {
        super(guard);
      }

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

  /**
   * Keeps an ORDER BY window-alias as a column reference (Sort is above the Project) to avoid a
   * second RexOver.
   */
  private static class ExtendedAstSortBuilder extends AstSortBuilder {

    ExtendedAstSortBuilder(QuerySpecification querySpec) {
      super(querySpec);
    }

    @Override
    public UnresolvedPlan visitOrderByClause(OrderByClauseContext ctx) {
      List<Field> fields = new ArrayList<>();
      List<UnresolvedExpression> items = querySpec.getOrderByItems();
      List<SortOption> options = querySpec.getOrderByOptions();
      for (int i = 0; i < items.size(); i++) {
        UnresolvedExpression item = items.get(i);
        UnresolvedExpression sortKey =
            (querySpec.isSelectAlias(item)
                    && querySpec.getSelectItemByAlias(item) instanceof WindowFunction)
                ? item
                : querySpec.replaceIfAliasOrOrdinal(item);
        fields.add(new Field(sortKey, createSortArguments(options.get(i))));
      }
      return new Sort(fields);
    }
  }
}
