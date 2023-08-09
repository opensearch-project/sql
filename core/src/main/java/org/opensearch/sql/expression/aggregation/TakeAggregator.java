/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import static org.opensearch.sql.utils.ExpressionUtils.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

/**
 * The take aggregator keeps and returns the original values of a field. If the field value is NULL
 * or MISSING, then it is skipped.
 */
public class TakeAggregator extends Aggregator<TakeAggregator.TakeState> {

  public TakeAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.TAKE.getName(), arguments, returnType);
  }

  @Override
  public TakeState create() {
    return new TakeState(getArguments().get(1).valueOf().integerValue());
  }

  @Override
  protected TakeState iterate(ExprValue value, TakeState state) {
    state.take(value);
    return state;
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "take(%s)", format(getArguments()));
  }

  /** Take State. */
  protected static class TakeState implements AggregationState {
    protected int index;
    protected int size;
    protected List<ExprValue> hits;

    TakeState(int size) {
      if (size <= 0) {
        throw new IllegalArgumentException("size must be greater than 0");
      }
      this.index = 0;
      this.size = size;
      this.hits = new ArrayList<>();
    }

    public void take(ExprValue value) {
      if (index < size) {
        hits.add(value);
      }
      index++;
    }

    @Override
    public ExprValue result() {
      return new ExprCollectionValue(hits);
    }
  }
}
