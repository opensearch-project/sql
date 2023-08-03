/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedAttribute;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * Use this unresolved plan visitor to check if a plan can be serialized by PaginatedPlanCache.<br>
 * If
 *
 * <pre>plan.accept(new CanPaginateVisitor(...))</pre>
 *
 * returns <em>true</em>,<br>
 * then PaginatedPlanCache.convertToCursor will succeed. Otherwise, it will fail.<br>
 * The purpose of this visitor is to activate legacy engine fallback mechanism.<br>
 * Currently, V2 engine does not support queries with:<br>
 * - aggregation (GROUP BY clause or aggregation functions like min/max)<br>
 * - in memory aggregation (window function)<br>
 * - LIMIT/OFFSET clause(s)<br>
 * - without FROM clause<br>
 * - JOIN<br>
 * - a subquery<br>
 * V2 also requires that the table being queried should be an OpenSearch index.<br>
 * See PaginatedPlanCache.canConvertToCursor for usage.
 */
public class CanPaginateVisitor extends AbstractNodeVisitor<Boolean, Object> {

  @Override
  public Boolean visitRelation(Relation node, Object context) {
    if (!node.getChild().isEmpty()) {
      // Relation instance should never have a child, but check just in case.
      return Boolean.FALSE;
    }

    return Boolean.TRUE;
  }

  protected Boolean canPaginate(Node node, Object context) {
    var childList = node.getChild();
    if (childList != null) {
      return childList.stream().allMatch(n -> n.accept(this, context));
    }
    return Boolean.TRUE;
  }

  // Only column references in ORDER BY clause are supported in pagination,
  // because expressions can't be pushed down due to #1471.
  // https://github.com/opensearch-project/sql/issues/1471
  @Override
  public Boolean visitSort(Sort node, Object context) {
    return node.getSortList().stream()
            .allMatch(f -> f.getField() instanceof QualifiedName && visitField(f, context))
        && canPaginate(node, context);
  }

  // For queries with WHERE clause:
  @Override
  public Boolean visitFilter(Filter node, Object context) {
    return canPaginate(node, context) && node.getCondition().accept(this, context);
  }

  // Queries with GROUP BY clause are not supported
  @Override
  public Boolean visitAggregation(Aggregation node, Object context) {
    return Boolean.FALSE;
  }

  // For queries without FROM clause:
  @Override
  public Boolean visitValues(Values node, Object context) {
    return Boolean.TRUE;
  }

  // Queries with LIMIT/OFFSET clauses are unsupported
  @Override
  public Boolean visitLimit(Limit node, Object context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitLiteral(Literal node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitField(Field node, Object context) {
    return canPaginate(node, context)
        && node.getFieldArgs().stream().allMatch(n -> n.accept(this, context));
  }

  @Override
  public Boolean visitAlias(Alias node, Object context) {
    return canPaginate(node, context) && node.getDelegated().accept(this, context);
  }

  @Override
  public Boolean visitAllFields(AllFields node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitQualifiedName(QualifiedName node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitEqualTo(EqualTo node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitRelevanceFieldList(RelevanceFieldList node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitInterval(Interval node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitCompare(Compare node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitNot(Not node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitOr(Or node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitAnd(And node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitArgument(Argument node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitXor(Xor node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitFunction(Function node, Object context) {
    // https://github.com/opensearch-project/sql/issues/1718
    if (node.getFuncName()
        .equalsIgnoreCase(BuiltinFunctionName.NESTED.getName().getFunctionName())) {
      return Boolean.FALSE;
    }
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitIn(In node, Object context) {
    return canPaginate(node, context)
        && node.getValueList().stream().allMatch(n -> n.accept(this, context));
  }

  @Override
  public Boolean visitBetween(Between node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitCase(Case node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitWhen(When node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitCast(Cast node, Object context) {
    return canPaginate(node, context) && node.getConvertedType().accept(this, context);
  }

  @Override
  public Boolean visitHighlightFunction(HighlightFunction node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitUnresolvedArgument(UnresolvedArgument node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitUnresolvedAttribute(UnresolvedAttribute node, Object context) {
    return canPaginate(node, context);
  }

  @Override
  public Boolean visitChildren(Node node, Object context) {
    // for all not listed (= unchecked) - false
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitWindowFunction(WindowFunction node, Object context) {
    // don't support in-memory aggregation
    // SELECT max(age) OVER (PARTITION BY city) ...
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    if (!node.getProjectList().stream().allMatch(n -> n.accept(this, context))) {
      return Boolean.FALSE;
    }

    var children = node.getChild();
    if (children.size() != 1) {
      return Boolean.FALSE;
    }

    return children.get(0).accept(this, context);
  }
}
