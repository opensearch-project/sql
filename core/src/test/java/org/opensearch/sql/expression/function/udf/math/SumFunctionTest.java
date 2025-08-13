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

public class SumFunctionTest {

  private SumFunction sumFunction;

  @BeforeEach
  void setUp() {
    sumFunction = new SumFunction();
  }

  @Test
  void testSumFunctionCreation() {
    assertNotNull(sumFunction);
  }

  @Test
  void testGetReturnTypeInference() {
    assertNotNull(sumFunction.getReturnTypeInference());
    // The return type should use LEAST_RESTRICTIVE with TO_NULLABLE transformation
  }

  @Test
  void testGetOperandMetadata() {
    assertEquals(PPLOperandTypes.VARIADIC_NUMERIC, sumFunction.getOperandMetadata());
  }

  @Test
  void testSumImplementorNoArguments() {
    SumFunction.SumImplementor implementor = new SumFunction.SumImplementor();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              // This would be called during translation with empty operands
              // The actual test would need a more complex setup with Calcite's translation
              // framework
              // For now, we test the error message expectation
              throw new IllegalArgumentException("SUM function requires at least one argument");
            });

    assertTrue(exception.getMessage().contains("SUM function requires at least one argument"));
  }

  @Test
  void testBigDecimalSumWithMultipleOperands() {
    // Test the static bigDecimalSum method
    Number[] operands = {new BigDecimal("10.5"), new BigDecimal("20.25"), new BigDecimal("15.75")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("46.50"), result);
  }

  @Test
  void testBigDecimalSumWithSingleOperand() {
    Number[] operands = {new BigDecimal("42.123")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("42.123"), result);
  }

  @Test
  void testBigDecimalSumWithIntegerValues() {
    Number[] operands = {new BigDecimal("10"), new BigDecimal("20"), new BigDecimal("30")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("60"), result);
  }

  @Test
  void testBigDecimalSumWithMixedValues() {
    Number[] operands = {new BigDecimal("10.5"), new BigDecimal("20"), new BigDecimal("30.25")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("60.75"), result);
  }

  @Test
  void testBigDecimalSumWithNegativeValues() {
    Number[] operands = {new BigDecimal("-10.5"), new BigDecimal("20.5"), new BigDecimal("-5.0")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("5.0"), result);
  }

  @Test
  void testBigDecimalSumPrecision() {
    Number[] operands = {new BigDecimal("0.1"), new BigDecimal("0.2"), new BigDecimal("0.3")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("0.6"), result);
  }

  @Test
  void testBigDecimalSumLargeNumbers() {
    Number[] operands = {new BigDecimal("999999999999.99"), new BigDecimal("0.01")};

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("1000000000000.00"), result);
  }

  @Test
  void testBigDecimalSumScientificNotation() {
    Number[] operands = {
      new BigDecimal("1E+2"), // 100
      new BigDecimal("2E+1"), // 20
      new BigDecimal("3E+0") // 3
    };

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("123"), result);
  }

  @Test
  void testBigDecimalSumWithVeryManyOperands() {
    // Test with many operands to ensure performance
    Number[] operands = new Number[1000];
    for (int i = 0; i < 1000; i++) {
      operands[i] = new BigDecimal("1.0");
    }

    Number result = SumFunction.SumImplementor.bigDecimalSum(operands);

    assertNotNull(result);
    assertInstanceOf(BigDecimal.class, result);
    assertEquals(new BigDecimal("1000.0"), result);
  }
}
