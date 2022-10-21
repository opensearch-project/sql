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

public class TakeAggregator extends Aggregator<TakeAggregator.TakeState> {

  public TakeAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(BuiltinFunctionName.TAKE.getName(), arguments, returnType);
  }

  @Override
  public TakeState create() {
    return new TakeState(getArguments().get(1).valueOf(null).integerValue(),
        getArguments().get(2).valueOf(null).integerValue());
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

  /**
   * Take State.
   */
  protected static class TakeState implements AggregationState {
    protected int index;
    protected int size;
    protected int from;
    protected List<ExprValue> hits;

    TakeState(int size, int from) {
      if (size <= 0) {
        throw new IllegalArgumentException("size must be greater than 0");
      }
      if (from < 0) {
        throw new IllegalArgumentException("from must be greater than or equal to 0");
      }
      this.index = 0;
      this.size = size;
      this.from = from;
      this.hits = new ArrayList<>();
    }

    public void take(ExprValue value) {
      if (index >= from && index < from + size) {
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
