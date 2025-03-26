/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class TakeAggFunction implements UserDefinedAggFunction<TakeAggFunction.TakeAccumulator> {

  @Override
  public TakeAccumulator init() {
    return new TakeAccumulator();
  }

  @Override
  public Object result(TakeAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public TakeAccumulator add(TakeAccumulator acc, Object... values) {
    Object candidateValue = values[0];
    int size = 0;
    if (values.length > 1) {
      size = (int) values[1];
    } else {
      size = 10;
    }
    if (size > acc.size()) {
      acc.add(candidateValue);
    }
    return acc;
  }

  public static class TakeAccumulator implements Accumulator {
    private List<Object> hits;

    public TakeAccumulator() {
      hits = new ArrayList<>();
    }

    @Override
    public Object value(Object... argList) {
      return hits;
    }

    public void add(Object value) {
      hits.add(value);
    }

    public int size() {
      return hits.size();
    }
  }
}
