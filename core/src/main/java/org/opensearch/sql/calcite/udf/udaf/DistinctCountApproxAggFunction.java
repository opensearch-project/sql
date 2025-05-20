/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import com.google.zetasketch.HyperLogLogPlusPlus;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class DistinctCountApproxAggFunction
    implements UserDefinedAggFunction<DistinctCountApproxAggFunction.HLLAccumulator> {

  @Override
  public HLLAccumulator init() {
    return new HLLAccumulator();
  }

  @Override
  public Object result(HLLAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public HLLAccumulator add(HLLAccumulator acc, Object... values) {
    for (Object value : values) {
      if (value != null) {
        acc.add(value.toString());
      }
    }
    return acc;
  }

  public static class HLLAccumulator implements UserDefinedAggFunction.Accumulator {
    private final HyperLogLogPlusPlus<String> hll;

    public HLLAccumulator() {
      this.hll = new HyperLogLogPlusPlus.Builder().buildForStrings();
    }

    public void add(String value) {
      hll.add(value);
    }

    @Override
    public Object value(Object... args) {
        return hll.result();
    }
  }
}
