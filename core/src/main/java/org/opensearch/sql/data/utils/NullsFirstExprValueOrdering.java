/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import com.google.common.collect.Ordering;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Idea from guava {@link Ordering}. The only difference is the special logic to handle {@link
 * org.opensearch.sql.data.model.ExprNullValue} and {@link
 * org.opensearch.sql.data.model.ExprMissingValue}
 */
@RequiredArgsConstructor
public class NullsFirstExprValueOrdering extends ExprValueOrdering {
  private final ExprValueOrdering ordering;

  @Override
  public int compare(ExprValue left, ExprValue right) {
    if (left == right) {
      return 0;
    }
    if (left.isNull() || left.isMissing()) {
      return RIGHT_IS_GREATER;
    }
    if (right.isNull() || right.isMissing()) {
      return LEFT_IS_GREATER;
    }
    return ordering.compare(left, right);
  }

  @Override
  public ExprValueOrdering reverse() {
    return ordering.reverse().nullsLast();
  }

  @Override
  public ExprValueOrdering nullsFirst() {
    return this;
  }

  @Override
  public ExprValueOrdering nullsLast() {
    return ordering.nullsLast();
  }
}
