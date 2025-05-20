/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class EarliestFunction
    implements UserDefinedAggFunction<EarliestFunction.EarliestAccumulator> {
  @Override
  public EarliestAccumulator init() {
    return new EarliestAccumulator();
  }

  @Override
  public Object result(EarliestAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public EarliestAccumulator add(EarliestAccumulator acc, Object... values) {
    acc.add(values[0]);
    return acc;
  }

  public static class EarliestAccumulator implements UserDefinedAggFunction.Accumulator {
    private String earliest;

    public EarliestAccumulator() {
      earliest = null;
    }

    @Override
    public Object value(Object... argList) {
      return earliest;
    }

    public void add(Object value) {
      if (!Objects.isNull(value)) {
        if (earliest == null) {
          earliest = String.valueOf(value);
        } else if (earliest.compareTo(value.toString()) > 0) {
          earliest = String.valueOf(value);
        }
      }
    }
  }
}
