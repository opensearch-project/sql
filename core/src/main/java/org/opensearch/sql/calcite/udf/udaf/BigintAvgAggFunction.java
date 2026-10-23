/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

/** BIGINT average aggregate that accumulates in double to avoid an intermediate long overflow. */
public class BigintAvgAggFunction {

  public static Accumulator init() {
    return new Accumulator();
  }

  public static Accumulator add(Accumulator accumulator, Long value) {
    if (value != null) {
      accumulator.sum += value;
      accumulator.count++;
    }
    return accumulator;
  }

  public static Double result(Accumulator accumulator) {
    return accumulator.count == 0 ? null : accumulator.sum / accumulator.count;
  }

  public static class Accumulator {
    private double sum;
    private long count;
  }
}
