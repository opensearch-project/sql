/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.SearchExpression;

/**
 * Logical plan node for Search operation. Represents search expressions that get converted to
 * query_string function.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Search extends UnresolvedPlan {

  private final UnresolvedPlan child;
  private final String queryString;
  private final SearchExpression originalExpression;

  @Override
  public List<UnresolvedPlan> getChild() {
    return ImmutableList.of(child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitSearch(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return new Search(child, queryString, originalExpression);
  }
}
