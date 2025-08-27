/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Search expression for AND operator. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchAnd extends SearchExpression {

  private final SearchExpression left;
  private final SearchExpression right;

  @Override
  public String toQueryString() {
    return left.toQueryString() + " AND " + right.toQueryString();
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Arrays.asList(left, right);
  }
}
