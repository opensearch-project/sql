/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.SearchExpression;
import org.opensearch.sql.ast.expression.SearchSubquery;

/**
 * Logical plan node for Search operation. Represents search expressions that get converted to
 * query_string function.
 */
@Getter
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@RequiredArgsConstructor
public class Search extends UnresolvedPlan {

  @EqualsAndHashCode.Include private final UnresolvedPlan child;
  @EqualsAndHashCode.Include private final @Nullable String queryString;

  // Currently it's only for anonymizer
  private final @Nullable SearchExpression originalExpression;

  public Search(UnresolvedPlan child, String queryString) {
    this(child, queryString, null);
  }

  /** Creates a search whose query string may need runtime subquery binding. */
  public static Search fromExpression(UnresolvedPlan child, SearchExpression originalExpression) {
    return new Search(
        child,
        containsSubquery(originalExpression) ? null : originalExpression.toQueryString(),
        originalExpression);
  }

  public boolean hasImplicitSubquery() {
    return queryString == null;
  }

  private static boolean containsSubquery(SearchExpression expression) {
    if (expression instanceof SearchSubquery) {
      return true;
    }
    return expression.getChild().stream()
        .filter(SearchExpression.class::isInstance)
        .map(SearchExpression.class::cast)
        .anyMatch(Search::containsSubquery);
  }

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
