/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Search expression for grouped expressions (parentheses). */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchGroup extends SearchExpression {

  private final SearchExpression expression;

  @Override
  public String toQueryString() {
    return "(" + expression.toQueryString() + ")";
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Collections.singletonList(expression);
  }
}
