/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
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
import org.opensearch.sql.ast.expression.HighlightFunction;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Map;
import org.opensearch.sql.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.ScoreFunction;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedAttribute;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.*;

/** AST nodes visitor Defines the traverse path. */
public abstract class AbstractNodeVisitor<T, C> {

  public T visit(Node node, C context) {
    return null;
  }

  /**
   * Visit child node.
   *
   * @param node {@link Node}
   * @param context Context
   * @return Return Type.
   */
  public T visitChildren(Node node, C context) {
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

  public T visitRelation(Relation node, C context) {
    return visitChildren(node, context);
  }

  public T visitRelationSubquery(RelationSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitTableFunction(TableFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitFilter(Filter node, C context) {
    return visitChildren(node, context);
  }

  public T visitTrendline(Trendline node, C context) {
    return visitChildren(node, context);
  }

  public T visitTrendlineComputation(Trendline.TrendlineComputation node, C context) {
    return visitChildren(node, context);
  }

  public T visitProject(Project node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregation(Aggregation node, C context) {
    return visitChildren(node, context);
  }

  public T visitEqualTo(EqualTo node, C context) {
    return visitChildren(node, context);
  }

  public T visitLiteral(Literal node, C context) {
    return visitChildren(node, context);
  }

  public T visitRelevanceFieldList(RelevanceFieldList node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedAttribute(UnresolvedAttribute node, C context) {
    return visitChildren(node, context);
  }

  public T visitAttributeList(AttributeList node, C context) {
    return visitChildren(node, context);
  }

  public T visitMap(Map node, C context) {
    return visitChildren(node, context);
  }

  public T visitNot(Not node, C context) {
    return visitChildren(node, context);
  }

  public T visitOr(Or node, C context) {
    return visitChildren(node, context);
  }

  public T visitAnd(And node, C context) {
    return visitChildren(node, context);
  }

  public T visitXor(Xor node, C context) {
    return visitChildren(node, context);
  }

  public T visitAggregateFunction(AggregateFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitFunction(Function node, C context) {
    return visitChildren(node, context);
  }

  public T visitWindowFunction(WindowFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitIn(In node, C context) {
    return visitChildren(node, context);
  }

  public T visitCompare(Compare node, C context) {
    return visitChildren(node, context);
  }

  public T visitBetween(Between node, C context) {
    return visitChildren(node, context);
  }

  public T visitArgument(Argument node, C context) {
    return visitChildren(node, context);
  }

  public T visitField(Field node, C context) {
    return visitChildren(node, context);
  }

  public T visitQualifiedName(QualifiedName node, C context) {
    return visitChildren(node, context);
  }

  public T visitRename(Rename node, C context) {
    return visitChildren(node, context);
  }

  public T visitEval(Eval node, C context) {
    return visitChildren(node, context);
  }

  public T visitParse(Parse node, C context) {
    return visitChildren(node, context);
  }

  public T visitLet(Let node, C context) {
    return visitChildren(node, context);
  }

  public T visitSort(Sort node, C context) {
    return visitChildren(node, context);
  }

  public T visitDedupe(Dedupe node, C context) {
    return visitChildren(node, context);
  }

  public T visitHead(Head node, C context) {
    return visitChildren(node, context);
  }

  public T visitRareTopN(RareTopN node, C context) {
    return visitChildren(node, context);
  }

  public T visitValues(Values node, C context) {
    return visitChildren(node, context);
  }

  public T visitAlias(Alias node, C context) {
    return visitChildren(node, context);
  }

  public T visitAllFields(AllFields node, C context) {
    return visitChildren(node, context);
  }

  public T visitAllFieldsExcludeMeta(AllFieldsExcludeMeta node, C context) {
    return visitChildren(node, context);
  }

  public T visitNestedAllTupleFields(NestedAllTupleFields node, C context) {
    return visitChildren(node, context);
  }

  public T visitInterval(Interval node, C context) {
    return visitChildren(node, context);
  }

  public T visitCase(Case node, C context) {
    return visitChildren(node, context);
  }

  public T visitWhen(When node, C context) {
    return visitChildren(node, context);
  }

  public T visitCast(Cast node, C context) {
    return visitChildren(node, context);
  }

  public T visitUnresolvedArgument(UnresolvedArgument node, C context) {
    return visitChildren(node, context);
  }

  public T visitLimit(Limit node, C context) {
    return visitChildren(node, context);
  }

  public T visitSpan(Span node, C context) {
    return visitChildren(node, context);
  }

  public T visitKmeans(Kmeans node, C context) {
    return visitChildren(node, context);
  }

  public T visitAD(AD node, C context) {
    return visitChildren(node, context);
  }

  public T visitML(ML node, C context) {
    return visitChildren(node, context);
  }

  public T visitHighlightFunction(HighlightFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitScoreFunction(ScoreFunction node, C context) {
    return visitChildren(node, context);
  }

  public T visitStatement(Statement node, C context) {
    return visit(node, context);
  }

  public T visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  public T visitExplain(Explain node, C context) {
    return visitStatement(node, context);
  }

  public T visitInSubquery(InSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitPaginate(Paginate paginate, C context) {
    return visitChildren(paginate, context);
  }

  public T visitFetchCursor(FetchCursor cursor, C context) {
    return visitChildren(cursor, context);
  }

  public T visitCloseCursor(CloseCursor closeCursor, C context) {
    return visitChildren(closeCursor, context);
  }

  public T visitFillNull(FillNull fillNull, C context) {
    return visitChildren(fillNull, context);
  }

  public T visitExpand(Expand expand, C context) {
    return visitChildren(expand, context);
  }

  public T visitPatterns(Patterns patterns, C context) {
    return visitChildren(patterns, context);
  }

  public T visitWindow(Window window, C context) {
    return visitChildren(window, context);
  }

  public T visitJoin(Join node, C context) {
    return visitChildren(node, context);
  }

  public T visitLookup(Lookup node, C context) {
    return visitChildren(node, context);
  }

  public T visitSubqueryAlias(SubqueryAlias node, C context) {
    return visitChildren(node, context);
  }

  public T visitScalarSubquery(ScalarSubquery node, C context) {
    return visitChildren(node, context);
  }

  public T visitExistsSubquery(ExistsSubquery node, C context) {
    return visitChildren(node, context);
  }
}
