/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class LatestFunction implements UserDefinedAggFunction<LatestFunction.LatestAccumulator> {
  @Override
  public LatestAccumulator init() {
    return new LatestAccumulator();
  }

  @Override
  public Object result(LatestFunction.LatestAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public LatestFunction.LatestAccumulator add(
      LatestFunction.LatestAccumulator acc, Object... values) {
    acc.add(values[0]);
    return acc;
  }

  public static class LatestAccumulator implements UserDefinedAggFunction.Accumulator {
    private String latest;

    public LatestAccumulator() {
      latest = null;
    }

    @Override
    public Object value(Object... argList) {
      return latest;
    }

    public void add(Object value) {
      if (!Objects.isNull(value)) {
        if (latest == null) {
          latest = String.valueOf(value);
        } else if (latest.compareTo(value.toString()) < 0) {
          latest = String.valueOf(value);
        }
      }
    }
  }
}
