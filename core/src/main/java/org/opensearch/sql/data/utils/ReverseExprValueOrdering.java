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
public class ReverseExprValueOrdering extends ExprValueOrdering {
  private final ExprValueOrdering forwardOrder;

  @Override
  public int compare(ExprValue left, ExprValue right) {
    return forwardOrder.compare(right, left);
  }

  @Override
  public ExprValueOrdering reverse() {
    return forwardOrder;
  }
}
