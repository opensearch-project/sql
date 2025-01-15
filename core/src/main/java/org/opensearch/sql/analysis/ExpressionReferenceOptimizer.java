/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.aggregation.Aggregator;
import org.opensearch.sql.expression.conditional.cases.CaseClause;
import org.opensearch.sql.expression.conditional.cases.WhenClause;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.OpenSearchFunctions;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalWindow;

/**
 * The optimizer used to replace the expression referred in the SelectClause</br> e.g. The query
 * SELECT abs(name), sum(age)-avg(age) FROM test GROUP BY abs(name).<br>
 * will be translated the AST<br>
 * Project[abs(age), sub(sum(age), avg(age))<br>
 * &ensp Agg(agg=[sum(age), avg(age)], group=[abs(age)]]<br>
 * &emsp Relation<br>
 * The sum(age) and avg(age) in the Project could be replaced by the analyzed reference, the
 * LogicalPlan should be<br>
 * LogicalProject[Ref("abs(age)"), sub(Ref("sum(age)"), Ref("avg(age)"))<br>
 * &ensp LogicalAgg(agg=[sum(age), avg(age)], group=[abs(age)]]<br>
 * &emsp LogicalRelation
 */
public class ExpressionReferenceOptimizer
    extends ExpressionNodeVisitor<Expression, AnalysisContext> {
  private final BuiltinFunctionRepository repository;

  /**
   * The map of expression and it's reference. For example, The NamedAggregator should produce the
   * map of Aggregator to Ref(name)
   */
  private final Map<Expression, Expression> expressionMap = new HashMap<>();

  public ExpressionReferenceOptimizer(
      BuiltinFunctionRepository repository, LogicalPlan logicalPlan) {
    this.repository = repository;
    logicalPlan.accept(new ExpressionMapBuilder(), null);
  }

  public Expression optimize(Expression analyzed, AnalysisContext context) {
    return analyzed.accept(this, context);
  }

  @Override
  public Expression visitNode(Expression node, AnalysisContext context) {
    return node;
  }

  @Override
  public Expression visitFunction(FunctionExpression node, AnalysisContext context) {
    if (expressionMap.containsKey(node)) {
      return expressionMap.get(node);
    } else {
      final List<Expression> args =
          node.getArguments().stream()
              .map(expr -> expr.accept(this, context))
              .collect(Collectors.toList());
      Expression optimizedFunctionExpression =
          (Expression)
              repository.compile(context.getFunctionProperties(), node.getFunctionName(), args);
      // Propagate scoreTracked for OpenSearch functions
      if (optimizedFunctionExpression instanceof OpenSearchFunctions.OpenSearchFunction) {
        ((OpenSearchFunctions.OpenSearchFunction) optimizedFunctionExpression)
            .setScoreTracked(((OpenSearchFunctions.OpenSearchFunction) node).isScoreTracked());
      }
      return optimizedFunctionExpression;
    }
  }

  @Override
  public Expression visitAggregator(Aggregator<?> node, AnalysisContext context) {
    return expressionMap.getOrDefault(node, node);
  }

  @Override
  public Expression visitNamed(NamedExpression node, AnalysisContext context) {
    if (expressionMap.containsKey(node)) {
      return expressionMap.get(node);
    }
    return node.getDelegated().accept(this, context);
  }

  /** Implement this because Case/When is not registered in function repository. */
  @Override
  public Expression visitCase(CaseClause node, AnalysisContext context) {
    if (expressionMap.containsKey(node)) {
      return expressionMap.get(node);
    }

    List<WhenClause> whenClauses =
        node.getWhenClauses().stream()
            .map(expr -> (WhenClause) expr.accept(this, context))
            .collect(Collectors.toList());
    Expression defaultResult = null;
    if (node.getDefaultResult() != null) {
      defaultResult = node.getDefaultResult().accept(this, context);
    }
    return new CaseClause(whenClauses, defaultResult);
  }

  @Override
  public Expression visitWhen(WhenClause node, AnalysisContext context) {
    return new WhenClause(
        node.getCondition().accept(this, context), node.getResult().accept(this, context));
  }

  /** Expression Map Builder. */
  class ExpressionMapBuilder extends LogicalPlanNodeVisitor<Void, Void> {

    @Override
    public Void visitNode(LogicalPlan plan, Void context) {
      plan.getChild().forEach(child -> child.accept(this, context));
      return null;
    }

    @Override
    public Void visitAggregation(LogicalAggregation plan, Void context) {
      // Create the mapping for all the aggregator.
      plan.getAggregatorList()
          .forEach(
              namedAggregator ->
                  expressionMap.put(
                      namedAggregator.getDelegated(),
                      new ReferenceExpression(namedAggregator.getName(), namedAggregator.type())));
      // Create the mapping for all the group by.
      plan.getGroupByList()
          .forEach(
              groupBy ->
                  expressionMap.put(
                      groupBy.getDelegated(),
                      new ReferenceExpression(groupBy.getName(), groupBy.type())));
      return null;
    }

    @Override
    public Void visitWindow(LogicalWindow plan, Void context) {
      Expression windowFunc = plan.getWindowFunction();
      expressionMap.put(
          windowFunc,
          new ReferenceExpression(((NamedExpression) windowFunc).getName(), windowFunc.type()));
      return visitNode(plan, context);
    }
  }
}
