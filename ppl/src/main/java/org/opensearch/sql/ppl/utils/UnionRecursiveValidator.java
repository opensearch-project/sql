/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.List;
import java.util.Locale;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.RelationSubquery;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnionRecursive;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.exception.SemanticCheckException;

public final class UnionRecursiveValidator {

  private UnionRecursiveValidator() {}

  public static void validate(UnresolvedPlan recursiveSubsearch, String relationName) {
    ValidationState state = new ValidationState(relationName);
    state.visit(recursiveSubsearch);
    if (!state.isRecursiveRelationReferenced()) {
      throw new SemanticCheckException(
          "UNION RECURSIVE subsearch must reference recursive relation: " + relationName);
    }
  }

  private static final class ValidationState {
    private final String relationName;
    private final String relationNameLower;
    private boolean recursiveRelationReferenced;

    private ValidationState(String relationName) {
      this.relationName = relationName;
      this.relationNameLower = relationName.toLowerCase(Locale.ROOT);
    }

    private boolean isRecursiveRelationReferenced() {
      return recursiveRelationReferenced;
    }

    private void visit(UnresolvedPlan plan) {
      if (plan == null) {
        return;
      }
      if (plan instanceof Relation relation) {
        visitRelation(relation);
        return;
      }
      if (plan instanceof SubqueryAlias alias) {
        visitSubqueryAlias(alias);
        return;
      }
      if (plan instanceof RelationSubquery relationSubquery) {
        visitRelationSubquery(relationSubquery);
        return;
      }
      if (plan instanceof Join join) {
        for (UnresolvedPlan child : join.getChildren()) {
          visit(child);
        }
        return;
      }
      if (plan instanceof Lookup lookup) {
        visitChildren(lookup.getChild());
        visit(lookup.getLookupRelation());
        return;
      }
      if (plan instanceof Append append) {
        visitChildren(append.getChild());
        visit(append.getSubSearch());
        return;
      }
      if (plan instanceof AppendPipe appendPipe) {
        visitChildren(appendPipe.getChild());
        visit(appendPipe.getSubQuery());
        return;
      }
      if (plan instanceof UnionRecursive unionRecursive) {
        visitChildren(unionRecursive.getChild());
        visit(unionRecursive.getRecursiveSubsearch());
        return;
      }
      visitChildren(plan.getChild());
    }

    private void visitChildren(List<? extends Node> children) {
      if (children == null) {
        return;
      }
      for (Node child : children) {
        if (child instanceof UnresolvedPlan planChild) {
          visit(planChild);
        }
      }
    }

    private void visitRelation(Relation relation) {
      List<QualifiedName> names = relation.getQualifiedNames();
      boolean containsRecursive = false;
      for (QualifiedName name : names) {
        if (isRecursiveQualifiedName(name)) {
          containsRecursive = true;
          break;
        }
      }
      if (containsRecursive) {
        if (names.size() > 1) {
          throw new SemanticCheckException(
              "UNION RECURSIVE relation name cannot be combined with other sources: "
                  + relationName);
        }
        recursiveRelationReferenced = true;
      }
    }

    private void visitSubqueryAlias(SubqueryAlias alias) {
      if (isRecursiveName(alias.getAlias())) {
        throw new SemanticCheckException(
            "UNION RECURSIVE relation name conflicts with alias: " + relationName);
      }
      visitChildren(alias.getChild());
    }

    private void visitRelationSubquery(RelationSubquery relationSubquery) {
      if (isRecursiveName(relationSubquery.getAliasAsTableName())) {
        throw new SemanticCheckException(
            "UNION RECURSIVE relation name conflicts with alias: " + relationName);
      }
      visitChildren(relationSubquery.getChild());
    }

    private boolean isRecursiveQualifiedName(QualifiedName name) {
      return name.getParts().size() == 1 && isRecursiveName(name.getParts().get(0));
    }

    private boolean isRecursiveName(String name) {
      return name != null && name.toLowerCase(Locale.ROOT).equals(relationNameLower);
    }
  }
}
