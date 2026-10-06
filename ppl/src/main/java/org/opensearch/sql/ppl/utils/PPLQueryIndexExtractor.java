/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

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
 * <p>Subqueries carried in <em>expressions</em> are not reachable that way, so {@link Filter} and
 * {@link Eval} conditions are bridged explicitly. A subquery in a {@link Join}'s ON condition is
 * still not walked and is dropped.
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
     */
    @Override
    public Void visitChildren(Node node, Set<String> names) {
      super.visitChildren(node, names);
      if (node instanceof UnresolvedPlan plan) {
        for (UnresolvedPlan source : plan.getSources()) {
          if (source != null) {
            source.accept(this, names);
          }
        }
      }
      return null;
    }

    @Override
    public Void visitFilter(Filter node, Set<String> names) {
      // The condition can hold a subquery (where id in [ source=b ... ]); the plan walk doesn't
      // descend into expressions, so bridge into it here.
      visitChildren(node, names);
      if (node.getCondition() != null) {
        node.getCondition().accept(this, names);
      }
      return null;
    }

    @Override
    public Void visitEval(Eval node, Set<String> names) {
      visitChildren(node, names);
      if (node.getExpressionList() != null) {
        for (Let let : node.getExpressionList()) {
          if (let != null && let.getExpression() != null) {
            let.getExpression().accept(this, names);
          }
        }
      }
      return null;
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
