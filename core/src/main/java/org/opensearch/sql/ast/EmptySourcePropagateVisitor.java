/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.ast.tree.UnionRecursive;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Values;

/** AST nodes visitor simplifies subsearch ast tree with empty source input. */
public class EmptySourcePropagateVisitor extends AbstractNodeVisitor<UnresolvedPlan, Void> {

  public static final UnresolvedPlan EMPTY_SOURCE = new Values(Collections.emptyList());

  @Override
  public UnresolvedPlan visitValues(Values node, Void context) {
    return node;
  }

  @Override
  public UnresolvedPlan visitRelation(Relation node, Void context) {
    return node;
  }

  // Assume future table functions like inputLookup, makeresult command will use this unresolved
  // plan
  @Override
  public UnresolvedPlan visitTableFunction(TableFunction node, Void context) {
    return node;
  }

  @Override
  public UnresolvedPlan visitChildren(Node node, Void context) {
    assert node instanceof UnresolvedPlan;
    UnresolvedPlan unresolvedPlan = (UnresolvedPlan) node;

    if (unresolvedPlan.getChild().size() == 1) {
      return isEmptySource(((List<UnresolvedPlan>) unresolvedPlan.getChild()).get(0))
          ? EMPTY_SOURCE
          : unresolvedPlan;
    }
    return super.visitChildren(node, context);
  }

  @Override
  public UnresolvedPlan visitAppend(Append node, Void context) {
    UnresolvedPlan subSearch = node.getSubSearch().accept(this, context);
    UnresolvedPlan child = node.getChild().get(0).accept(this, context);
    return new Append(subSearch).attach(child);
  }

  @Override
  public UnresolvedPlan visitAppendCol(AppendCol node, Void context) {
    UnresolvedPlan subSearch = node.getSubSearch().accept(this, context);
    UnresolvedPlan child = node.getChild().get(0).accept(this, context);
    return new AppendCol(node.isOverride(), subSearch).attach(child);
  }

  @Override
  public UnresolvedPlan visitUnionRecursive(UnionRecursive node, Void context) {
    UnresolvedPlan recursiveSubsearch = node.getRecursiveSubsearch().accept(this, context);
    UnresolvedPlan child = node.getChild().get(0).accept(this, context);
    return new UnionRecursive(
            node.getRelationName(), node.getMaxDepth(), node.getMaxRows(), recursiveSubsearch)
        .attach(child);
  }

  // TODO: Revisit lookup logic here but for now we don't see use case yet
  @Override
  public UnresolvedPlan visitLookup(Lookup node, Void context) {
    UnresolvedPlan lookupRelation = node.getLookupRelation().accept(this, context);
    UnresolvedPlan child = node.getChild().get(0).accept(this, context);
    // Lookup is a LEFT join.
    // If left child is expected to be 0 row, it outputs 0 row
    // If right child is expected to be 0 row, the output is the left child;
    if (isEmptySource(child)) {
      return EMPTY_SOURCE;
    }
    return isEmptySource(lookupRelation) ? child : node;
  }

  // Not see use case yet
  @Override
  public UnresolvedPlan visitJoin(Join node, Void context) {
    UnresolvedPlan left = node.getLeft().accept(this, context);
    UnresolvedPlan right = node.getRight().accept(this, context);

    boolean leftEmpty = isEmptySource(left);
    boolean rightEmpty = isEmptySource(right);

    switch (node.getJoinType()) {
      case INNER:
      case CROSS:
        return leftEmpty || rightEmpty ? EMPTY_SOURCE : node;
      case LEFT:
      case SEMI:
      case ANTI:
        if (leftEmpty) {
          return EMPTY_SOURCE;
        }
        return rightEmpty ? left : node;
      case RIGHT:
        if (rightEmpty) {
          return EMPTY_SOURCE;
        }
        return leftEmpty ? right : node;
      case FULL:
        if (leftEmpty) {
          return right;
        }
        return rightEmpty ? left : node;
      default:
        return node;
    }
  }

  private boolean isEmptySource(UnresolvedPlan plan) {
    return plan instanceof Values
        && (((Values) plan).getValues() == null || ((Values) plan).getValues().isEmpty());
  }
}
