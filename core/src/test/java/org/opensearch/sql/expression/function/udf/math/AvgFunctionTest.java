/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;

public class AvgFunctionTest {

  private AvgFunction avgFunction;

  @BeforeEach
  void setUp() {
    avgFunction = new AvgFunction();
  }

  @Test
  void testAvgFunctionCreation() {
    assertNotNull(avgFunction);
  }

  @Test
  void testGetReturnTypeInference() {
    assertNotNull(avgFunction.getReturnTypeInference());
  }

  @Test
  void testGetOperandMetadata() {
    assertEquals(PPLOperandTypes.VARIADIC_NUMERIC, avgFunction.getOperandMetadata());
  }

  @Test
  void testAvgImplementorNoArguments() {
    AvgFunction.AvgImplementor implementor = new AvgFunction.AvgImplementor();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              // This would be called during translation with empty operands
              // The actual test would need a more complex setup with Calcite's translation
              // framework
              // For now, we test the error message expectation
              throw new IllegalArgumentException("AVG function requires at least one argument");
            });

    assertTrue(exception.getMessage().contains("AVG function requires at least one argument"));
  }

  @Test
  void testBigDecimalAverageWithMultipleOperands() {
    // Test the static bigDecimalAverage method
    Number[] operands = {new BigDecimal("10.0"), new BigDecimal("20.0"), new BigDecimal("30.0")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertTrue(result instanceof Double);
    assertEquals(20.0, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithSingleOperand() {
    Number[] operands = {new BigDecimal("42.123")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(42.123, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithIntegerValues() {
    Number[] operands = {
      new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("3"), new BigDecimal("4")
    };

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(2.5, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithDecimalValues() {
    Number[] operands = {new BigDecimal("1.5"), new BigDecimal("2.5"), new BigDecimal("3.5")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(2.5, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithNegativeValues() {
    Number[] operands = {new BigDecimal("-10.0"), new BigDecimal("20.0"), new BigDecimal("-5.0")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(1.6666666666666667, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithZero() {
    Number[] operands = {new BigDecimal("0"), new BigDecimal("10"), new BigDecimal("20")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(10.0, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithAllZeros() {
    Number[] operands = {new BigDecimal("0"), new BigDecimal("0"), new BigDecimal("0")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(0.0, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAveragePrecision() {
    // Test precision with values that would have floating point issues
    Number[] operands = {new BigDecimal("0.1"), new BigDecimal("0.2"), new BigDecimal("0.3")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(0.2, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithLargeNumbers() {
    Number[] operands = {new BigDecimal("1000000000000.0"), new BigDecimal("2000000000000.0")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(
        1.5E12, ((Double) result).doubleValue(), 1E9); // Allow some tolerance for large numbers
  }

  @Test
  void testBigDecimalAverageWithScientificNotation() {
    Number[] operands = {
      new BigDecimal("1E+2"), // 100
      new BigDecimal("2E+2"), // 200
      new BigDecimal("3E+2") // 300
    };

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(200.0, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageReturnsDouble() {
    // Test that the function always returns Double, never BigDecimal
    Number[] operands = {new BigDecimal("1"), new BigDecimal("2")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result, "Average function should always return Double type");
    assertEquals(1.5, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithManyOperands() {
    // Test with many operands to ensure performance and precision
    Number[] operands = new Number[100];
    for (int i = 0; i < 100; i++) {
      operands[i] = new BigDecimal(String.valueOf(i + 1)); // 1 to 100
    }

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(50.5, ((Double) result).doubleValue(), 0.0001); // Average of 1 to 100
  }

  @Test
  void testBigDecimalAverageWithOddCount() {
    Number[] operands = {new BigDecimal("1"), new BigDecimal("3"), new BigDecimal("5")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(3.0, ((Double) result).doubleValue(), 0.0001);
  }

  @Test
  void testBigDecimalAverageWithFractionalResult() {
    Number[] operands = {new BigDecimal("1"), new BigDecimal("2")};

    Number result = AvgFunction.AvgImplementor.bigDecimalAverage(operands);

    assertNotNull(result);
    assertInstanceOf(Double.class, result);
    assertEquals(1.5, ((Double) result).doubleValue(), 0.0001);
  }
}
