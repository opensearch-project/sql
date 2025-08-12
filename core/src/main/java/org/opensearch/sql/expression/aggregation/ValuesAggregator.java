/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.aggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Aggregator for the values() function that collects unique values into a list,
 * sorted lexicographically with no limit.
 */
public class ValuesAggregator extends Aggregator<ValuesAggregator.ValuesState> {

  public ValuesAggregator(List<Expression> arguments, ExprCoreType returnType) {
    super(FunctionName.of("values"), arguments, returnType);
  }

  @Override
  public ValuesState create() {
    return new ValuesState();
  }

  @Override
  protected ValuesState iterate(ExprValue value, ValuesState state) {
    if (!value.isNull() && !value.isMissing()) {
      state.addValue(value);
    }
    return state;
  }

  protected static class ValuesState implements AggregationState {
    private final Set<String> uniqueValues = new TreeSet<>(); // TreeSet for lexicographical ordering

    public void addValue(ExprValue value) {
      uniqueValues.add(String.valueOf(value.value()));
    }

    @Override
    public ExprValue result() {
      List<ExprValue> exprValues = new ArrayList<>();
      for (String value : uniqueValues) {
        exprValues.add(new ExprStringValue(value));
      }
      return ExprTupleValue.fromExprValueMap(
          java.util.Map.of("values", new org.opensearch.sql.data.model.ExprCollectionValue(exprValues)));
    }
  }
}