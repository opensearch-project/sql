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
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.GraphLookup;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Collects the source index name(s) a PPL query reads from by walking the parsed AST (via {@link
 * Relation#getQualifiedNames()}) instead of regex-matching text, which mishandles multi-index and
 * datasource-qualified sources.
 *
 * <p>The default visitor only follows {@code getChild()}, so a source held in any other field is
 * reached by an explicit override: a {@link Join}'s left/right branches, a {@link Lookup} table,
 * the sub-search of {@link Append}/{@link AppendCol}/{@link AppendPipe}, a {@link GraphLookup}'s
 * {@code from} table, and subquery relations embedded in {@link Filter}/{@link Eval} condition
 * expressions. Returns distinct names in encounter order. This is best-effort metadata; a source it
 * cannot resolve is simply omitted.
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

    @Override
    public Void visitJoin(Join node, Set<String> names) {
      // getChild() exposes only the left branch (and only once attached, so it may be null); visit
      // both sides explicitly, guarding the nullable left.
      if (node.getLeft() != null) {
        node.getLeft().accept(this, names);
      }
      node.getRight().accept(this, names);
      return null;
    }

    @Override
    public Void visitLookup(Lookup node, Set<String> names) {
      // getChild() returns only the piped input; the lookup table is a separate relation.
      super.visitChildren(node, names);
      node.getLookupRelation().accept(this, names);
      return null;
    }

    @Override
    public Void visitAppend(Append node, Set<String> names) {
      super.visitChildren(node, names);
      acceptIfPresent(node.getSubSearch(), names);
      return null;
    }

    @Override
    public Void visitAppendCol(AppendCol node, Set<String> names) {
      super.visitChildren(node, names);
      acceptIfPresent(node.getSubSearch(), names);
      return null;
    }

    @Override
    public Void visitAppendPipe(AppendPipe node, Set<String> names) {
      super.visitChildren(node, names);
      acceptIfPresent(node.getSubQuery(), names);
      return null;
    }

    @Override
    public Void visitGraphLookup(GraphLookup node, Set<String> names) {
      super.visitChildren(node, names);
      acceptIfPresent(node.getFromTable(), names);
      return null;
    }

    @Override
    public Void visitFilter(Filter node, Set<String> names) {
      // The condition can hold a subquery (where id in [ source=b ... ]); the plan walk doesn't
      // descend into it, so bridge into the expression here.
      super.visitChildren(node, names);
      if (node.getCondition() != null) {
        node.getCondition().accept(this, names);
      }
      return null;
    }

    @Override
    public Void visitEval(Eval node, Set<String> names) {
      super.visitChildren(node, names);
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

    private void acceptIfPresent(UnresolvedPlan plan, Set<String> names) {
      if (plan != null) {
        plan.accept(this, names);
      }
    }
  }
}
