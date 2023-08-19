/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.parse.ParseExpression;

/**
 * Abstract visitor for expression tree nodes.
 *
 * @param <T> type of return value to accumulate when visiting.
 * @param <C> type of context.
 */
public abstract class ExpressionNodeVisitor<T, C> {

  public T visitNode(Expression node, C context) {
    return null;
  }

  /**
   * Visit children nodes in function arguments.
   *
   * @param node function node
   * @param context context
   * @return result
   */
  public T visitChildren(FunctionImplementation node, C context) {
    T result = defaultResult();

    for (Expression child : node.getArguments()) {
      T childResult = child.accept(this, context);
      result = aggregateResult(result, childResult);
    }
    return result;
  }

  private T defaultResult() {
    return null;
  }

  private T aggregateResult(T aggregate, T nextResult) {
    return nextResult;
  }

  public T visitLiteral(LiteralExpression node, C context) {
    return visitNode(node, context);
  }

  public T visitNamed(NamedExpression node, C context) {
    return node.getDelegated().accept(this, context);
  }

  public T visitReference(ReferenceExpression node, C context) {
    return visitNode(node, context);
  }

  public T visitParse(ParseExpression node, C context) {
    return visitNode(node, context);
  }

  public T visitFunction(FunctionExpression node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregator(Aggregator<?> node, C context) {
    return visitChildren(node, context);
  }

  public T visitNamedAggregator(NamedAggregator node, C context) {
    return visitChildren(node, context);
  }

  /**
   * Call visitFunction() by default rather than visitChildren(). This makes CASE/WHEN able to be
   * handled:
   *
   * <ol>
   *   <li>by visitFunction() if not overwritten: ex. FilterQueryBuilder
   *   <li>by visitCase/When() otherwise if any special logic: ex. ExprReferenceOptimizer
   * </ol>
   */
  public T visitCase(CaseClause node, C context) {
    return visitFunction(node, context);
  }

  public T visitWhen(WhenClause node, C context) {
    return visitFunction(node, context);
  }

  public T visitNamedArgument(NamedArgumentExpression node, C context) {
    return visitNode(node, context);
  }
}
