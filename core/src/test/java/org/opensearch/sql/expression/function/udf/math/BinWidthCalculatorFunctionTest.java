/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class BinWidthCalculatorFunctionTest {

  @Test
  public void testCalculateOptimalWidth() {
    // Test case 1: Range 30, max 50, bins 3 should return width 100
    // Width 10: CEIL(30/10)=3, 50%10=0 so extraBin=1, actualBins=4 > 3 → FAIL
    // Width 100: CEIL(30/100)=1, 50%100=50≠0 so extraBin=0, actualBins=1 ≤ 3 → PASS
    Double result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            30.0, 50.0, 3);
    assertEquals(100.0, result, 0.001, "For range 30, max 50, bins 3, should return width 100");

    // Test case 2: Range 100, max 100, bins 5 should return width 100
    // Width 100: CEIL(100/100)=1, 100%100=0 so extraBin=1, actualBins=2 ≤ 5 → PASS
    result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            100.0, 100.0, 5);
    assertEquals(100.0, result, 0.001, "For range 100, max 100, bins 5, should return width 100");

    // Test case 3: Range 20, max 25, bins 3 should return width 10
    // Width 10: CEIL(20/10)=2, 25%10=5≠0 so extraBin=0, actualBins=2 ≤ 3 → PASS
    result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            20.0, 25.0, 3);
    assertEquals(10.0, result, 0.001, "For range 20, max 25, bins 3, should return width 10");
  }

  @Test
  public void testCalculateOptimalWidthWithExtraBin() {
    // Test max_value % width == 0 condition (needs extra bin)
    // Range 30, max 60, bins 4: width 10, 60%10=0 so extraBin=1, actualBins=4 ≤ 4 → PASS
    Double result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            30.0, 60.0, 4);
    assertEquals(10.0, result, 0.001, "With extra bin, should still fit in 4 bins");

    // Range 30, max 60, bins 3: width 10, 60%10=0 so extraBin=1, actualBins=4 > 3 → FAIL
    // Should move to width 100: CEIL(30/100)=1, 60%100=60≠0 so extraBin=0, actualBins=1 ≤ 3 → PASS
    result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            30.0, 60.0, 3);
    assertEquals(100.0, result, 0.001, "Extra bin condition should force larger width");
  }

  @Test
  public void testCalculateOptimalWidthFallback() {
    // Test fallback when no nice width works
    // Very large range with few bins should fall back to exact calculation
    // The largest nice width is 1E9, so if we need something larger, it should fallback
    Double result =
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            1E12, 1E12, 1);
    assertEquals(
        1E12 / 1, result, 1E6, "Should fallback to exact width calculation for very large ranges");
  }

  @Test
  public void testCalculateOptimalWidthNullInputs() {
    // Test null inputs
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            null, 100.0, 3));
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            100.0, null, 3));
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            100.0, 100.0, null));
  }

  @Test
  public void testCalculateOptimalWidthInvalidInputs() {
    // Test invalid inputs
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            -10.0, 100.0, 3),
        "Negative range should return null");
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            100.0, 100.0, 0),
        "Zero bins should return null");
    assertNull(
        BinWidthCalculatorFunction.BinWidthCalculatorImplementor.calculateOptimalWidth(
            0.0, 100.0, 3),
        "Zero range should return null");
  }
}
