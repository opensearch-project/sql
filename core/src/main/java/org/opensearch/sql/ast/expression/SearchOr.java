/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;

/** Search expression for OR operator. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchOr extends SearchExpression {

  private final SearchExpression left;
  private final SearchExpression right;

  @Override
  public String toQueryString(Function<String, ExprType> fieldTypeResolver) {
    return left.toQueryString(fieldTypeResolver) + " OR " + right.toQueryString(fieldTypeResolver);
  }

  @Override
  public String toAnonymizedString() {
    return left.toAnonymizedString() + " OR " + right.toAnonymizedString();
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Arrays.asList(left, right);
  }
}
