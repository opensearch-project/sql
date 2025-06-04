/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class DistinctCountApproxAggFunction
    implements UserDefinedAggFunction<DistinctCountApproxAggFunction.HLLAccumulator> {

  @Override
  public DistinctCountApproxAggFunction.HLLAccumulator init() {
    return new DistinctCountApproxAggFunction.HLLAccumulator();
  }

  @Override
  public Object result(DistinctCountApproxAggFunction.HLLAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public DistinctCountApproxAggFunction.HLLAccumulator add(
      DistinctCountApproxAggFunction.HLLAccumulator acc, Object... values) {
    for (Object value : values) {
      if (value != null) {
        acc.add(value);
      }
    }
    return acc;
  }

  public static class HLLAccumulator implements UserDefinedAggFunction.Accumulator {
    private final HyperLogLogPlusPlus hll;

    public HLLAccumulator() {
      this.hll =
          new HyperLogLogPlusPlus(
              HyperLogLogPlusPlus.DEFAULT_PRECISION, BigArrays.NON_RECYCLING_INSTANCE, 1);
    }

    public void add(Object value) {
      hll.collect(0, hash(value));
    }

    @Override
    public Object value(Object... args) {
      return hll.cardinality(0);
    }
  }

  private static long hash(Object data) {
    MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
    if (data == null) {
      return 0L;
    }

    byte[] bytes;

    if (data instanceof byte[]) {
      bytes = (byte[]) data;
    } else if (data instanceof String) {
      bytes = ((String) data).getBytes(StandardCharsets.UTF_8);
    } else if (data instanceof Number) {
      long value = ((Number) data).longValue();
      bytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    } else {
      bytes = data.toString().getBytes(StandardCharsets.UTF_8);
    }

    MurmurHash3.hash128(bytes, 0, bytes.length, 0, hash);
    return hash.h1 ^ hash.h2;
  }
}
