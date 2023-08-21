/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.context;

import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.FilteredAggregationFunctionCallContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.GroupByElementContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.OrderByElementContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectClauseContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectElementContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SubqueryAsRelationContext;
import static org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.WindowFunctionClauseContext;
import static org.opensearch.sql.sql.parser.ParserUtils.createSortOption;
import static org.opensearch.sql.sql.parser.ParserUtils.getTextInQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.AggregateFunctionCallContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.QuerySpecificationContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SelectSpecContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParserBaseVisitor;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;

/**
 * Query specification domain that collects basic info for a simple query.
 *
 * <pre>
 * (I) What is the impact of this new abstraction?
 *  This abstraction and collecting process turns AST building process into two phases:
 *  1) Query specification collects info
 *  2) AST builder uses the info to build AST node
 *
 * (II) Why is this required?
 *  There are two reasons as follows that make single pass building hard or impossible:
 *  1) Building aggregation AST node needs to know all aggregate function calls
 *     in SELECT and HAVING clause
 *  2) GROUP BY or HAVING clause may contain aliases defined in SELECT clause
 *
 * </pre>
 */
@EqualsAndHashCode
@Getter
@ToString
public class QuerySpecification {

  /** Items in SELECT clause and mapping from alias to select item. */
  private final List<UnresolvedExpression> selectItems = new ArrayList<>();

  private final Map<String, UnresolvedExpression> selectItemsByAlias = new HashMap<>();

  /**
   * Aggregate function calls that spreads in SELECT, HAVING clause. Since this is going to be
   * pushed to aggregation operator, de-duplicate is necessary to avoid duplication.
   */
  private final Set<UnresolvedExpression> aggregators = new LinkedHashSet<>();

  /**
   *
   *
   * <pre>
   * Items in GROUP BY clause that may be:
   *  1) Simple field name
   *  2) Field nested in scalar function call
   *  3) Ordinal that points to expression in SELECT
   *  4) Alias that points to expression in SELECT.
   *  </pre>
   */
  private final List<UnresolvedExpression> groupByItems = new ArrayList<>();

  /** Items in ORDER BY clause that may be different forms as above and its options. */
  private final List<UnresolvedExpression> orderByItems = new ArrayList<>();

  private final List<SortOption> orderByOptions = new ArrayList<>();

  /**
   * Collect all query information in the parse tree excluding info in sub-query).
   *
   * @param query query spec node in parse tree
   */
  public void collect(QuerySpecificationContext query, String queryString) {
    query.accept(new QuerySpecificationCollector(queryString));
  }

  /**
   * Replace unresolved expression if it's an alias or ordinal that represents an actual expression
   * in SELECT list.
   *
   * @param expr item to be replaced
   * @return select item that the given expr represents
   */
  public UnresolvedExpression replaceIfAliasOrOrdinal(UnresolvedExpression expr) {
    if (isIntegerLiteral(expr)) {
      return getSelectItemByOrdinal(expr);
    } else if (isSelectAlias(expr)) {
      return getSelectItemByAlias(expr);
    } else {
      return expr;
    }
  }

  private boolean isIntegerLiteral(UnresolvedExpression expr) {
    if (!(expr instanceof Literal)) {
      return false;
    }

    if (((Literal) expr).getType() != DataType.INTEGER) {
      throw new SemanticCheckException(
          StringUtils.format("Non-integer constant [%s] found in ordinal", expr));
    }
    return true;
  }

  private UnresolvedExpression getSelectItemByOrdinal(UnresolvedExpression expr) {
    int ordinal = (Integer) ((Literal) expr).getValue();
    if (ordinal <= 0 || ordinal > selectItems.size()) {
      throw new SemanticCheckException(
          StringUtils.format("Ordinal [%d] is out of bound of select item list", ordinal));
    }
    return selectItems.get(ordinal - 1);
  }

  /**
   * Check if an expression is a select alias.
   *
   * @param expr expression
   * @return true if it's an alias
   */
  public boolean isSelectAlias(UnresolvedExpression expr) {
    return (expr instanceof QualifiedName) && (selectItemsByAlias.containsKey(expr.toString()));
  }

  /**
   * Get original expression aliased in SELECT clause.
   *
   * @param expr alias
   * @return expression in SELECT
   */
  public UnresolvedExpression getSelectItemByAlias(UnresolvedExpression expr) {
    return selectItemsByAlias.get(expr.toString());
  }

  /*
   * Query specification collector that visits a parse tree to collect query info.
   * Most visit methods only collect info and returns nothing. However, one exception is
   * visitQuerySpec() which needs to change visit ordering to avoid visiting sub-query.
   */
  private class QuerySpecificationCollector extends OpenSearchSQLParserBaseVisitor<Void> {
    private final AstExpressionBuilder expressionBuilder = new AstExpressionBuilder();

    private final String queryString;

    public QuerySpecificationCollector(String queryString) {
      this.queryString = queryString;
    }

    @Override
    public Void visitSubqueryAsRelation(SubqueryAsRelationContext ctx) {
      // skip collecting subquery for current layer
      return null;
    }

    @Override
    public Void visitWindowFunctionClause(WindowFunctionClauseContext ctx) {
      // skip collecting sort items in window functions
      return null;
    }

    @Override
    public Void visitSelectClause(SelectClauseContext ctx) {
      super.visitSelectClause(ctx);

      // SELECT DISTINCT is an equivalent and special form of GROUP BY
      if (isDistinct(ctx.selectSpec())) {
        groupByItems.addAll(selectItems);
      }
      return null;
    }

    @Override
    public Void visitSelectElement(SelectElementContext ctx) {
      UnresolvedExpression expr = visitAstExpression(ctx.expression());
      selectItems.add(expr);

      if (ctx.alias() != null) {
        String alias = StringUtils.unquoteIdentifier(ctx.alias().getText());
        selectItemsByAlias.put(alias, expr);
      }
      return super.visitSelectElement(ctx);
    }

    @Override
    public Void visitGroupByElement(GroupByElementContext ctx) {
      groupByItems.add(visitAstExpression(ctx));
      return super.visitGroupByElement(ctx);
    }

    @Override
    public Void visitOrderByElement(OrderByElementContext ctx) {
      orderByItems.add(visitAstExpression(ctx.expression()));
      orderByOptions.add(createSortOption(ctx));
      return super.visitOrderByElement(ctx);
    }

    @Override
    public Void visitAggregateFunctionCall(AggregateFunctionCallContext ctx) {
      aggregators.add(AstDSL.alias(getTextInQuery(ctx, queryString), visitAstExpression(ctx)));
      return super.visitAggregateFunctionCall(ctx);
    }

    @Override
    public Void visitFilteredAggregationFunctionCall(FilteredAggregationFunctionCallContext ctx) {
      UnresolvedExpression aggregateFunction = visitAstExpression(ctx);
      aggregators.add(AstDSL.alias(getTextInQuery(ctx, queryString), aggregateFunction));
      return super.visitFilteredAggregationFunctionCall(ctx);
    }

    private boolean isDistinct(SelectSpecContext ctx) {
      return (ctx != null) && (ctx.DISTINCT() != null);
    }

    private UnresolvedExpression visitAstExpression(ParseTree tree) {
      return expressionBuilder.visit(tree);
    }
  }
}
