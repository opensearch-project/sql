/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.List;
import java.util.Locale;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The last aggregator returns the last value of a field in natural document order. NULL and MISSING
 * values are skipped.
 */
public class LastAggregator extends Aggregator<LastAggregator.LastState> {

  public LastAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.LAST.getName(), arguments, returnType);
  }

  @Override
  public LastState create() {
    return new LastState();
  }

  @Override
  protected LastState iterate(ExprValue value, LastState state) {
    state.setLast(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "last(%s)", format(getArguments()));
  }

  /** Last State. */
  protected static class LastState implements AggregationState {
    private ExprValue last = null;

    public void setLast(ExprValue value) {
      last = value;
    }

    @Override
    public ExprValue result() {
      return last != null ? last : ExprNullValue.of();
    }
  }
}
