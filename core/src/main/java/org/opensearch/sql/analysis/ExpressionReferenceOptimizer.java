/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.common.utils.StringUtils.format;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.opensearch.sql.exception.SemanticCheckException;
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
import org.opensearch.sql.planner.logical.LogicalRelation;
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

  private String leftRelationName;
  private String rightRelationName;
  private Set<String> leftSideAttributes;
  private Set<String> rightSideAttributes;

  public ExpressionReferenceOptimizer(
      BuiltinFunctionRepository repository, LogicalPlan logicalPlan) {
    this.repository = repository;
    logicalPlan.accept(new ExpressionMapBuilder(), null);
  }

  public ExpressionReferenceOptimizer(
      BuiltinFunctionRepository repository, LogicalPlan... logicalPlans) {
    this.repository = repository;
    // To resolve join condition, we store left side and left side of join.
    if (logicalPlans.length == 2) {
      // TODO current implementation only support two-tables join, so we can directly convert them
      // to LogicalRelation. To support two-plans join, we can get the LogicalRelation by searching.
      this.leftRelationName = ((LogicalRelation) logicalPlans[0]).getRelationName();
      this.rightRelationName = ((LogicalRelation) logicalPlans[1]).getRelationName();
      this.leftSideAttributes =
          ((LogicalRelation) logicalPlans[0]).getTable().getFieldTypes().keySet();
      this.rightSideAttributes =
          ((LogicalRelation) logicalPlans[1]).getTable().getFieldTypes().keySet();
    }
    Arrays.stream(logicalPlans).forEach(p -> p.accept(new ExpressionMapBuilder(), null));
  }

  public Expression optimize(Expression analyzed, AnalysisContext context) {
    return analyzed.accept(this, context);
  }

  @Override
  public Expression visitNode(Expression node, AnalysisContext context) {
    return node;
  }

  /**
   * Add index prefix to reference attribute of join condition. The attribute could be: case 1:
   * Field -> Index.Field case 2: Field.Field -> Index.Field.Field case 3: .Index.Field,
   * .Index.Field.Field -> do nothing case 4: Index.Field, Index.Field.Field -> do nothing
   */
  @Override
  public Expression visitReference(ReferenceExpression node, AnalysisContext context) {
    if (leftRelationName == null || rightRelationName == null) {
      return node;
    }

    String attr = node.getAttr();
    // case 1 or case 2
    if (!attr.contains(".") || (!attr.startsWith(".") && !isIndexPrefix(attr))) {
      return replaceReferenceExpressionWithIndexPrefix(node, attr);
    }
    return node;
  }

  private ReferenceExpression replaceReferenceExpressionWithIndexPrefix(
      ReferenceExpression node, String attr) {
    if (leftSideAttributes.contains(attr) && rightSideAttributes.contains(attr)) {
      throw new SemanticCheckException(format("Reference `%s` is ambiguous", attr));
    } else if (leftSideAttributes.contains(attr)) {
      return new ReferenceExpression(format("%s.%s", leftRelationName, attr), node.type());
    } else if (rightSideAttributes.contains(attr)) {
      return new ReferenceExpression(format("%s.%s", rightRelationName, attr), node.type());
    } else {
      return node;
    }
  }

  private boolean isIndexPrefix(String attr) {
    int separator = attr.indexOf('.');
    String possibleIndexPrefix = attr.substring(0, separator);
    return leftRelationName.contains(possibleIndexPrefix)
        || rightRelationName.contains(possibleIndexPrefix);
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
                      new ReferenceExpression(groupBy.getNameOrAlias(), groupBy.type())));
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
