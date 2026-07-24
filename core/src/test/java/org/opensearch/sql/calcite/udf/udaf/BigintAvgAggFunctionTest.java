/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

class BigintAvgAggFunctionTest {

  @Test
  void retainsAvgKindForPushdownRules() {
    assertEquals(SqlKind.AVG, PPLBuiltinOperators.BIGINT_AVG.getKind());
  }

  @Test
  void averagesWithoutLongOverflow() {
    BigintAvgAggFunction.Accumulator accumulator = BigintAvgAggFunction.init();
    accumulator = BigintAvgAggFunction.add(accumulator, Long.MAX_VALUE);
    accumulator = BigintAvgAggFunction.add(accumulator, Long.MAX_VALUE);

    assertEquals((double) Long.MAX_VALUE, BigintAvgAggFunction.result(accumulator));
  }

  @Test
  void ignoresNulls() {
    BigintAvgAggFunction.Accumulator accumulator = BigintAvgAggFunction.init();
    accumulator = BigintAvgAggFunction.add(accumulator, null);
    accumulator = BigintAvgAggFunction.add(accumulator, 10L);

    assertEquals(10D, BigintAvgAggFunction.result(accumulator));
  }

  @Test
  void returnsNullForEmptyInput() {
    assertNull(BigintAvgAggFunction.result(BigintAvgAggFunction.init()));
  }
}
