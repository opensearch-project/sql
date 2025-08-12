/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Aggregator for the list() function that collects values into a list,
 * preserving duplicates and order, with a limit of 100 items.
 */
public class ListAggregator extends Aggregator<ListAggregator.ListState> {

  private static final int DEFAULT_LIMIT = 100;

  public ListAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(FunctionName.of("list"), arguments, returnType);
  }

  @Override
  public ListState create() {
    return new ListState();
  }

  @Override
  protected ListState iterate(ExprValue value, ListState state) {
    if (!value.isNull() && !value.isMissing()) {
      state.addValue(value);
    }
    return state;
  }

  protected static class ListState implements AggregationState {
    private final List<String> values = new ArrayList<>();

    public void addValue(ExprValue value) {
      if (values.size() < DEFAULT_LIMIT) {
        values.add(String.valueOf(value.value()));
      }
    }

    @Override
    public ExprValue result() {
      List<ExprValue> exprValues = new ArrayList<>();
      for (String value : values) {
        exprValues.add(new ExprStringValue(value));
      }
      return ExprTupleValue.fromExprValueMap(
          java.util.Map.of("list", new org.opensearch.sql.data.model.ExprCollectionValue(exprValues)));
    }
  }
}