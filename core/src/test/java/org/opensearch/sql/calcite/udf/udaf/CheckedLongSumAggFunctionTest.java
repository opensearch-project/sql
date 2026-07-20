/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CheckedLongSumAggFunctionTest {

  @Test
  void sumsExactly() {
    long accumulator = CheckedLongSumAggFunction.init();
    accumulator = CheckedLongSumAggFunction.add(accumulator, 1L << 62);
    accumulator = CheckedLongSumAggFunction.add(accumulator, 1L);

    assertEquals((1L << 62) + 1L, CheckedLongSumAggFunction.result(accumulator));
  }

  @Test
  void throwsOnPositiveOverflow() {
    assertThrows(
        ArithmeticException.class, () -> CheckedLongSumAggFunction.add(Long.MAX_VALUE, 1L));
  }

  @Test
  void throwsOnNegativeOverflow() {
    assertThrows(
        ArithmeticException.class, () -> CheckedLongSumAggFunction.add(Long.MIN_VALUE, -1L));
  }
}
