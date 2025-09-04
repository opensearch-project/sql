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
 * The first aggregator returns the first value of a field in natural document order. NULL and
 * MISSING values are skipped.
 */
public class FirstAggregator extends Aggregator<FirstAggregator.FirstState> {

  public FirstAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.FIRST.getName(), arguments, returnType);
  }

  @Override
  public FirstState create() {
    return new FirstState();
  }

  @Override
  protected FirstState iterate(ExprValue value, FirstState state) {
    state.setFirst(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "first(%s)", format(getArguments()));
  }

  /** First State. */
  protected static class FirstState implements AggregationState {
    private ExprValue first = null;
    private boolean hasValue = false;

    public void setFirst(ExprValue value) {
      if (!hasValue) {
        first = value;
        hasValue = true;
      }
    }

    @Override
    public ExprValue result() {
      return hasValue ? first : ExprNullValue.of();
    }
  }
}
