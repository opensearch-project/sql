/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage.bindingtuple;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Lazy Implementation of {@link BindingTuple}.
 */
@RequiredArgsConstructor
public class LazyBindingTuple extends BindingTuple {
  private final Supplier<ExprTupleValue> lazyBinding;

  @Override
  public ExprValue resolve(ReferenceExpression ref) {
    return ref.resolve(lazyBinding.get());
  }
}
