/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

/** BIGINT sum aggregate that throws when its running long accumulator overflows. */
public class CheckedLongSumAggFunction {

  public static long init() {
    return 0L;
  }

  public static long add(long accumulator, long value) {
    return Math.addExact(accumulator, value);
  }

  public static long result(long accumulator) {
    return accumulator;
  }
}
