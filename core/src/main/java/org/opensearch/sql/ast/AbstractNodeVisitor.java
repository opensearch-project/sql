/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.AttributeList;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedAttribute;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.AD;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FetchCursor;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Kmeans;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.ML;
import org.opensearch.sql.ast.tree.Paginate;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.Values;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;

import java.util.List;

/** AST nodes visitor Defines the traverse path. */
public interface AbstractNodeVisitor<T, C> {

  default T visit(Node node, C context) {
    return null;
  }

  /**
   * Visit child node.
   *
   * @param node {@link Node}
   * @param context Context
   * @return Return Type.
   */
  default T visitChildren(Node node, C context) {
    T result = defaultResult();

    for (Node child : node.getChild()) {
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

  default T visitRelation(Relation node, C context) {
    return visitChildren(node, context);
  }

  default T visitRelationSubquery(RelationSubquery node, C context) {
    return visitChildren(node, context);
  }

  default T visitTableFunction(TableFunction node, C context) {
    return visitChildren(node, context);
  }

  default T visitFilter(Filter node, C context) {
    return visitChildren(node, context);
  }

  default T visitProject(Project node, C context) {
    return visitChildren(node, context);
  }

  // TODO maybe pass columns and namedExpressions thru context
  default T visitProjectList(List<UnresolvedExpression> columns, List<NamedExpression> namedExpressions, T node, C context) {
    return null;
  }

  default void verifySupportsCondition(Expression condition) {}

  default T visitAggregation(Aggregation node, C context) {
    return visitChildren(node, context);
  }

  default T visitEqualTo(EqualTo node, C context) {
    return visitChildren(node, context);
  }

  default T visitLiteral(Literal node, C context) {
    return visitChildren(node, context);
  }

  default T visitUnresolvedAttribute(UnresolvedAttribute node, C context) {
    return visitChildren(node, context);
  }

  default T visitAttributeList(AttributeList node, C context) {
    return visitChildren(node, context);
  }

  default T visitMap(Map node, C context) {
    return visitChildren(node, context);
  }

  default T visitNot(Not node, C context) {
    return visitChildren(node, context);
  }

  default T visitOr(Or node, C context) {
    return visitChildren(node, context);
  }

  default T visitAnd(And node, C context) {
    return visitChildren(node, context);
  }

  default T visitXor(Xor node, C context) {
    return visitChildren(node, context);
  }

  default T visitAggregateFunction(AggregateFunction node, C context) {
    return visitChildren(node, context);
  }

  default T visitFunction(Function node, C context) {
    return visitChildren(node, context);
  }

  default T visitWindowFunction(WindowFunction node, C context) {
    return visitChildren(node, context);
  }

  default T visitIn(In node, C context) {
    return visitChildren(node, context);
  }

  default T visitCompare(Compare node, C context) {
    return visitChildren(node, context);
  }

  default T visitBetween(Between node, C context) {
    return visitChildren(node, context);
  }

  default T visitArgument(Argument node, C context) {
    return visitChildren(node, context);
  }

  default T visitField(Field node, C context) {
    return visitChildren(node, context);
  }

  default T visitQualifiedName(QualifiedName node, C context) {
    return visitChildren(node, context);
  }

  default T visitRename(Rename node, C context) {
    return visitChildren(node, context);
  }

  default T visitEval(Eval node, C context) {
    return visitChildren(node, context);
  }

  default T visitParse(Parse node, C context) {
    return visitChildren(node, context);
  }

  default T visitLet(Let node, C context) {
    return visitChildren(node, context);
  }

  default T visitSort(Sort node, C context) {
    return visitChildren(node, context);
  }

  default T visitDedupe(Dedupe node, C context) {
    return visitChildren(node, context);
  }

  default T visitHead(Head node, C context) {
    return visitChildren(node, context);
  }

  default T visitRareTopN(RareTopN node, C context) {
    return visitChildren(node, context);
  }

  default T visitValues(Values node, C context) {
    return visitChildren(node, context);
  }

  default T visitAlias(Alias node, C context) {
    return visitChildren(node, context);
  }

  default T visitAllFields(AllFields node, C context) {
    return visitChildren(node, context);
  }

  default T visitInterval(Interval node, C context) {
    return visitChildren(node, context);
  }

  default T visitCase(Case node, C context) {
    return visitChildren(node, context);
  }

  default T visitWhen(When node, C context) {
    return visitChildren(node, context);
  }

  default T visitCast(Cast node, C context) {
    return visitChildren(node, context);
  }

  default T visitUnresolvedArgument(UnresolvedArgument node, C context) {
    return visitChildren(node, context);
  }

  default T visitLimit(Limit node, C context) {
    return visitChildren(node, context);
  }

  default T visitSpan(Span node, C context) {
    return visitChildren(node, context);
  }

  default T visitKmeans(Kmeans node, C context) {
    return visitChildren(node, context);
  }

  default T visitAD(AD node, C context) {
    return visitChildren(node, context);
  }

  default T visitML(ML node, C context) {
    return visitChildren(node, context);
  }

  default T visitStatement(Statement node, C context) {
    return visit(node, context);
  }

  default T visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  default T visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  default T visitPaginate(Paginate paginate, C context) {
    return visitChildren(paginate, context);
  }

  default T visitFetchCursor(FetchCursor cursor, C context) {
    return visitChildren(cursor, context);
  }

  default T visitCloseCursor(CloseCursor closeCursor, C context) {
    return visitChildren(closeCursor, context);
  }
}
