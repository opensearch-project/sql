/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import com.google.common.collect.Ordering;
import java.util.Comparator;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Idea from guava {@link Ordering}. The only difference is the special logic to handle {@link
 * org.opensearch.sql.data.model.ExprNullValue} and {@link
 * org.opensearch.sql.data.model.ExprMissingValue}
 */
@RequiredArgsConstructor
public abstract class ExprValueOrdering implements Comparator<ExprValue> {

  public static ExprValueOrdering natural() {
    return NaturalExprValueOrdering.INSTANCE;
  }

  public ExprValueOrdering reverse() {
    return new ReverseExprValueOrdering(this);
  }

  public ExprValueOrdering nullsFirst() {
    return new NullsFirstExprValueOrdering(this);
  }

  public ExprValueOrdering nullsLast() {
    return new NullsLastExprValueOrdering(this);
  }

  // Never make these public
  static final int LEFT_IS_GREATER = 1;
  static final int RIGHT_IS_GREATER = -1;
}
