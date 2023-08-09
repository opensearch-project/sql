/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import com.google.common.collect.Ordering;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Idea from guava {@link Ordering}. The only difference is the special logic to handle {@link
 * org.opensearch.sql.data.model.ExprNullValue} and {@link
 * org.opensearch.sql.data.model.ExprMissingValue}
 */
public class NaturalExprValueOrdering extends ExprValueOrdering {
  static final ExprValueOrdering INSTANCE = new NaturalExprValueOrdering();

  private transient ExprValueOrdering nullsFirst;
  private transient ExprValueOrdering nullsLast;

  @Override
  public int compare(ExprValue left, ExprValue right) {
    return left.compareTo(right);
  }

  @Override
  public ExprValueOrdering nullsFirst() {
    ExprValueOrdering result = nullsFirst;
    if (result == null) {
      result = nullsFirst = super.nullsFirst();
    }
    return result;
  }

  @Override
  public ExprValueOrdering nullsLast() {
    ExprValueOrdering result = nullsLast;
    if (result == null) {
      result = nullsLast = super.nullsLast();
    }
    return result;
  }

  @Override
  public ExprValueOrdering reverse() {
    return super.reverse();
  }
}
