/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/** A subsearch used as a predicate term by the parent {@code search} command. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchSubquery extends SearchExpression {

  private final UnresolvedPlan query;

  @Override
  public String toQueryString() {
    throw new IllegalStateException(
        "An implicit search subquery must be planned as a correlated runtime input");
  }

  @Override
  public String toAnonymizedString() {
    return "[ subsearch ]";
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return List.of();
  }
}
