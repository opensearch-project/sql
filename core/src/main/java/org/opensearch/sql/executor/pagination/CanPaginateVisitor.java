/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Values;

/**
 * Use this unresolved plan visitor to check if a plan can be serialized by PaginatedPlanCache.
 * If plan.accept(new CanPaginateVisitor(...)) returns true,
 * then PaginatedPlanCache.convertToCursor will succeed. Otherwise, it will fail.
 * The purpose of this visitor is to activate legacy engine fallback mechanism.
 * Currently, the conditions are:
 * - only projection of a relation is supported.
 * - projection only has * (a.k.a. allFields).
 * - Relation only scans one table.
 * - The table is an open search index.
 * - Query has <pre>ORDER BY</pre> clause, which refers columns only by name or by ordinal
 *   (without any expression there).
 * - Query has no FROM clause.
 * So it accepts only queries like
 * <pre>SELECT * FROM $index [ORDER BY $col1, [$col2]] [LIMIT n]</pre>
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
    return node.getSortList().stream().allMatch(f -> f.getField() instanceof QualifiedName)
        // TODO visit fields after #1500 merge https://github.com/opensearch-project/sql/pull/1500
        && canPaginate(node, context);
  }

  // Queries without FROM clause are also supported
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
  public Boolean visitChildren(Node node, Object context) {
    return Boolean.FALSE;
  }

  @Override
  public Boolean visitProject(Project node, Object context) {
    var projections = node.getProjectList();
    if (projections.size() != 1) {
      return Boolean.FALSE;
    }

    if (!(projections.get(0) instanceof AllFields)) {
      return Boolean.FALSE;
    }

    var children = node.getChild();
    if (children.size() != 1) {
      return Boolean.FALSE;
    }

    return children.get(0).accept(this, context);
  }
}
