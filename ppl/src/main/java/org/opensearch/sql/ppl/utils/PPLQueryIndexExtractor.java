/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Foreach;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;

/**
 * Collects the source index name(s) a PPL query reads from by walking the parsed AST (via {@link
 * Relation#getQualifiedNames()}) instead of regex-matching text, which mishandles multi-index and
 * datasource-qualified sources.
 *
 * <p>Plan sources are found generically: the walk follows {@code getChild()} plus {@link
 * UnresolvedPlan#getSources()}, which each node uses to declare a plan it holds in a field (a
 * sub-search, a lookup table, a join's right branch). A new command therefore only has to declare
 * its sources on the node itself to be covered here.
 *
 * <p>Subqueries carried in <em>expressions</em> are not reachable that way, so every command whose
 * expression the engine evaluates is bridged explicitly: {@link Filter} and {@link Join} ON
 * conditions, {@link Eval}, {@link Foreach} and {@link FillNull} assignments, and {@link
 * Aggregation}, {@link Window}, {@link StreamWindow} and {@link Chart} function arguments. A
 * subquery in any of these positions parses and, except under {@code eventstats}, executes, so its
 * inner source is a real read and belongs in the metadata. Commands that take only field names,
 * literals or patterns cannot carry one.
 *
 * <p>Returns distinct names in encounter order. This is best-effort metadata; a source it cannot
 * resolve is simply omitted and query execution is unaffected.
 */
public final class PPLQueryIndexExtractor {

  private PPLQueryIndexExtractor() {}

  /** Return the distinct source index name(s) referenced by {@code plan}, in encounter order. */
  public static List<String> extractIndexNames(UnresolvedPlan plan) {
    Set<String> names = new LinkedHashSet<>();
    if (plan != null) {
      plan.accept(new Collector(), names);
    }
    return new ArrayList<>(names);
  }

  private static final class Collector extends AbstractNodeVisitor<Void, Set<String>> {
    @Override
    public Void visitRelation(Relation node, Set<String> names) {
      for (QualifiedName qualifiedName : node.getQualifiedNames()) {
        names.add(qualifiedName.toString());
      }
      return null;
    }

    /**
     * Walks {@code getChild()} as usual, then the node's declared {@link
     * UnresolvedPlan#getSources()}. Commands whose source lives in a field are covered without this
     * collector knowing each one.
     *
     * <p>{@code Node.getChild()} defaults to null and several expression nodes keep that default,
     * so the walk treats null as "no children" rather than relying on the base visitor.
     */
    @Override
    public Void visitChildren(Node node, Set<String> names) {
      List<? extends Node> children = node.getChild();
      if (children != null) {
        for (Node child : children) {
          if (child != null) {
            child.accept(this, names);
          }
        }
      }
      if (node instanceof UnresolvedPlan plan) {
        for (UnresolvedPlan source : plan.getSources()) {
          if (source != null) {
            source.accept(this, names);
          }
        }
      }
      return null;
    }

    // Expression bridges. Each command below evaluates an expression that may hold a subquery; the
    // plan walk does not descend into expressions, so hand each one to visit(). Null-safety lives
    // in
    // visit()/visitAll() alone so a hand-built AST with a missing part costs nothing.

    @Override
    public Void visitFilter(Filter node, Set<String> names) {
      visitChildren(node, names);
      visit(node.getCondition(), names);
      return null;
    }

    @Override
    public Void visitEval(Eval node, Set<String> names) {
      visitChildren(node, names);
      if (node.getExpressionList() != null) {
        for (Let let : node.getExpressionList()) {
          visit(let == null ? null : let.getExpression(), names);
        }
      }
      return null;
    }

    @Override
    public Void visitAlias(Alias node, Set<String> names) {
      // Alias keeps Node's null getChild(), and every aggregation argument and most eval
      // assignments arrive wrapped in one, so a subquery beneath it would otherwise be invisible.
      visit(node.getDelegated(), names);
      return null;
    }

    @Override
    public Void visitAggregation(Aggregation node, Set<String> names) {
      visitChildren(node, names);
      visitAll(node.getAggExprList(), names);
      visitAll(node.getGroupExprList(), names);
      visitAll(node.getSortExprList(), names);
      visit(node.getSpan(), names);
      return null;
    }

    @Override
    public Void visitWindow(Window node, Set<String> names) {
      visitChildren(node, names);
      visitAll(node.getWindowFunctionList(), names);
      visitAll(node.getGroupList(), names);
      return null;
    }

    @Override
    public Void visitStreamWindow(StreamWindow node, Set<String> names) {
      visitChildren(node, names);
      visitAll(node.getWindowFunctionList(), names);
      visitAll(node.getGroupList(), names);
      visit(node.getResetBefore(), names);
      visit(node.getResetAfter(), names);
      return null;
    }

    @Override
    public Void visitChart(Chart node, Set<String> names) {
      visitChildren(node, names);
      visit(node.getAggregationFunction(), names);
      visit(node.getRowSplit(), names);
      visit(node.getColumnSplit(), names);
      return null;
    }

    @Override
    public Void visitFillNull(FillNull node, Set<String> names) {
      visitChildren(node, names);
      visit(node.getReplacementForAll().orElse(null), names);
      if (node.getReplacementPairs() != null) {
        for (Pair<Field, UnresolvedExpression> pair : node.getReplacementPairs()) {
          visit(pair == null ? null : pair.getRight(), names);
        }
      }
      return null;
    }

    @Override
    public Void visitForeach(Foreach node, Set<String> names) {
      visitChildren(node, names);
      visit(node.getCollectionExpression(), names);
      if (node.getEvalClauses() != null) {
        for (Foreach.ForeachEvalClause clause : node.getEvalClauses()) {
          visit(clause == null ? null : clause.getExpression(), names);
        }
      }
      return null;
    }

    @Override
    public Void visitJoin(Join node, Set<String> names) {
      // Both sides come through getChild() + getSources(); the ON condition is an expression and
      // can carry a subquery (on l.a = r.a AND r.a in [ source=inner | ... ]).
      visitChildren(node, names);
      visit(node.getJoinCondition() == null ? null : node.getJoinCondition().orElse(null), names);
      return null;
    }

    private void visit(UnresolvedExpression expr, Set<String> names) {
      if (expr != null) {
        expr.accept(this, names);
      }
    }

    private void visitAll(List<? extends UnresolvedExpression> exprs, Set<String> names) {
      if (exprs != null) {
        for (UnresolvedExpression expr : exprs) {
          visit(expr, names);
        }
      }
    }

    @Override
    public Void visitScalarSubquery(ScalarSubquery node, Set<String> names) {
      node.getQuery().accept(this, names);
      return null;
    }

    @Override
    public Void visitInSubquery(InSubquery node, Set<String> names) {
      node.getQuery().accept(this, names);
      return null;
    }

    @Override
    public Void visitExistsSubquery(ExistsSubquery node, Set<String> names) {
      node.getQuery().accept(this, names);
      return null;
    }
  }
}
