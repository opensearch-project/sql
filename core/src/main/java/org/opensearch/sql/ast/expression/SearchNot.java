/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;

/** Search expression for NOT operator. */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class SearchNot extends SearchExpression {

  private final SearchExpression expression;

  @Override
  public String toQueryString(Function<String, ExprType> fieldTypeResolver) {
    return "NOT(" + expression.toQueryString(fieldTypeResolver) + ")";
  }

  @Override
  public String toAnonymizedString() {
    return "NOT(" + expression.toAnonymizedString() + ")";
  }

  @Override
  public List<? extends UnresolvedExpression> getChild() {
    return Collections.singletonList(expression);
  }
}
