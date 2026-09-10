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
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Collects the source index name(s) a PPL query reads from by walking the parsed AST (via {@link
 * Relation#getQualifiedNames()}) instead of regex-matching text, which mishandles multi-index and
 * datasource-qualified sources. Traversal also follows a {@link Join}'s right branch and subquery
 * relations, so join, lookup, and subsearch sources are captured. Returns distinct names in
 * encounter order.
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
      // getChild() returns only the left branch; visit both so the right source is captured.
      node.getLeft().accept(this, names);
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
