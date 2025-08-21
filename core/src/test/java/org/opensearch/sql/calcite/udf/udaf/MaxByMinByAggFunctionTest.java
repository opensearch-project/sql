/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class MaxByMinByAggFunctionTest {

  @Test
  public void testMaxByAggFunction() {
    MaxByAggFunction maxByFunction = new MaxByAggFunction();
    MaxByAggFunction.MaxByAccumulator accumulator = maxByFunction.init();

    maxByFunction.add(accumulator, "value1", 10);
    maxByFunction.add(accumulator, "value2", 20);
    maxByFunction.add(accumulator, "value3", 15);

    assertEquals("value2", maxByFunction.result(accumulator));
  }

  @Test
  public void testMaxByWithNullOrderValue() {
    MaxByAggFunction maxByFunction = new MaxByAggFunction();
    MaxByAggFunction.MaxByAccumulator accumulator = maxByFunction.init();

    maxByFunction.add(accumulator, "value1", null);
    maxByFunction.add(accumulator, "value2", 20);
    maxByFunction.add(accumulator, "value3", null);

    assertEquals("value2", maxByFunction.result(accumulator));
  }

  @Test
  public void testMaxByWithInsufficientArguments() {
    MaxByAggFunction maxByFunction = new MaxByAggFunction();
    MaxByAggFunction.MaxByAccumulator accumulator = maxByFunction.init();

    assertThrows(IllegalArgumentException.class, () -> {
      maxByFunction.add(accumulator, "value1");
    });
  }

  @Test
  public void testMinByAggFunction() {
    MinByAggFunction minByFunction = new MinByAggFunction();
    MinByAggFunction.MinByAccumulator accumulator = minByFunction.init();

    minByFunction.add(accumulator, "value1", 10);
    minByFunction.add(accumulator, "value2", 5);
    minByFunction.add(accumulator, "value3", 15);

    assertEquals("value2", minByFunction.result(accumulator));
  }

  @Test
  public void testMinByWithNullOrderValue() {
    MinByAggFunction minByFunction = new MinByAggFunction();
    MinByAggFunction.MinByAccumulator accumulator = minByFunction.init();

    minByFunction.add(accumulator, "value1", null);
    minByFunction.add(accumulator, "value2", 5);
    minByFunction.add(accumulator, "value3", null);

    assertEquals("value2", minByFunction.result(accumulator));
  }

  @Test
  public void testMinByWithInsufficientArguments() {
    MinByAggFunction minByFunction = new MinByAggFunction();
    MinByAggFunction.MinByAccumulator accumulator = minByFunction.init();

    assertThrows(IllegalArgumentException.class, () -> {
      minByFunction.add(accumulator, "value1");
    });
  }

  @Test
  public void testEmptyAccumulator() {
    MaxByAggFunction maxByFunction = new MaxByAggFunction();
    MaxByAggFunction.MaxByAccumulator maxAccumulator = maxByFunction.init();
    
    MinByAggFunction minByFunction = new MinByAggFunction();
    MinByAggFunction.MinByAccumulator minAccumulator = minByFunction.init();

    assertNull(maxByFunction.result(maxAccumulator));
    assertNull(minByFunction.result(minAccumulator));
  }

  @Test
  public void testWithStringOrderValues() {
    MaxByAggFunction maxByFunction = new MaxByAggFunction();
    MaxByAggFunction.MaxByAccumulator accumulator = maxByFunction.init();

    maxByFunction.add(accumulator, "apple", "zebra");
    maxByFunction.add(accumulator, "banana", "alpha");
    maxByFunction.add(accumulator, "cherry", "beta");

    assertEquals("apple", maxByFunction.result(accumulator));
  }
}
