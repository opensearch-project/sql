/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.sql.type.ReturnTypes;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;

public class ToNumberFunctionTest {

  private final ToNumberFunction function = new ToNumberFunction();

  @Test
  void testGetReturnTypeInference() {
    assertEquals(ReturnTypes.DOUBLE_FORCE_NULLABLE, function.getReturnTypeInference());
  }

  @Test
  void testGetOperandMetadata() {
    assertEquals(PPLOperandTypes.STRING_OR_STRING_INTEGER, function.getOperandMetadata());
  }

  @Test
  void testToNumberWithDefaultBase() {
    assertEquals(123L, ToNumberFunction.toNumber("123"));
    assertEquals(0L, ToNumberFunction.toNumber("0"));
    assertEquals(-456L, ToNumberFunction.toNumber("-456"));
    assertEquals(123.45, ToNumberFunction.toNumber("123.45"));
    assertEquals(-123.45, ToNumberFunction.toNumber("-123.45"));
    assertEquals(0.5, ToNumberFunction.toNumber("0.5"));
    assertEquals(-0.5, ToNumberFunction.toNumber("-0.5"));
  }

  @Test
  void testToNumberWithBase10() {
    assertEquals(123L, ToNumberFunction.toNumber("123", 10));
    assertEquals(0L, ToNumberFunction.toNumber("0", 10));
    assertEquals(-456L, ToNumberFunction.toNumber("-456", 10));
    assertEquals(123.45, ToNumberFunction.toNumber("123.45", 10));
    assertEquals(-123.45, ToNumberFunction.toNumber("-123.45", 10));
  }

  @Test
  void testToNumberWithBase2() {
    assertEquals(5L, ToNumberFunction.toNumber("101", 2));
    assertEquals(0L, ToNumberFunction.toNumber("0", 2));
    assertEquals(1L, ToNumberFunction.toNumber("1", 2));
    assertEquals(7L, ToNumberFunction.toNumber("111", 2));
    assertEquals(10L, ToNumberFunction.toNumber("1010", 2));
  }

  @Test
  void testToNumberWithBase8() {
    assertEquals(64L, ToNumberFunction.toNumber("100", 8));
    assertEquals(8L, ToNumberFunction.toNumber("10", 8));
    assertEquals(83L, ToNumberFunction.toNumber("123", 8));
    assertEquals(511L, ToNumberFunction.toNumber("777", 8));
  }

  @Test
  void testToNumberWithBase16() {
    assertEquals(255L, ToNumberFunction.toNumber("FF", 16));
    assertEquals(16L, ToNumberFunction.toNumber("10", 16));
    assertEquals(171L, ToNumberFunction.toNumber("AB", 16));
    assertEquals(291L, ToNumberFunction.toNumber("123", 16));
    assertEquals(4095L, ToNumberFunction.toNumber("FFF", 16));
  }

  @Test
  void testToNumberWithBase36() {
    assertEquals(35L, ToNumberFunction.toNumber("Z", 36));
    assertEquals(1295L, ToNumberFunction.toNumber("ZZ", 36));
    assertEquals(46655L, ToNumberFunction.toNumber("ZZZ", 36));
  }

  @Test
  void testToNumberWithDecimalBase2() {
    assertEquals(2L, ToNumberFunction.toNumber("10", 2));
    assertEquals(1L, ToNumberFunction.toNumber("1", 2));
    assertEquals(3L, ToNumberFunction.toNumber("11", 2));
  }

  @Test
  void testToNumberWithDecimalBase16() {
    assertEquals(255L, ToNumberFunction.toNumber("FF", 16));
    assertEquals(16L, ToNumberFunction.toNumber("10", 16));
    assertEquals(171L, ToNumberFunction.toNumber("AB", 16));
  }

  @Test
  void testToNumberWithNegativeDecimal() {
    assertEquals(-2L, ToNumberFunction.toNumber("-10", 2));
    assertEquals(-255L, ToNumberFunction.toNumber("-FF", 16));
    assertEquals(-123.45, ToNumberFunction.toNumber("-123.45", 10));
  }

  @Test
  void testToNumberWithEmptyFractionalPart() {
    assertEquals(123.0, ToNumberFunction.toNumber("123.", 10));
    assertEquals(255L, ToNumberFunction.toNumber("FF", 16));
    assertEquals(5L, ToNumberFunction.toNumber("101", 2));
  }

  @Test
  void testToNumberWithZeroIntegerPart() {
    assertEquals(0.5, ToNumberFunction.toNumber("0.5", 10));
    assertEquals(0L, ToNumberFunction.toNumber("0", 2));
  }

  @Test
  void testToNumberInvalidBase() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ToNumberFunction.toNumber("123", 1);
        });

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ToNumberFunction.toNumber("123", 37);
        });

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ToNumberFunction.toNumber("123", 0);
        });

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          ToNumberFunction.toNumber("123", -1);
        });
  }

  @Test
  void testToNumberInvalidDigits() {
    assertEquals(null, ToNumberFunction.toNumber("12A", 10));
    assertEquals(null, ToNumberFunction.toNumber("102", 2));
    assertEquals(null, ToNumberFunction.toNumber("101.101", 2));
    assertEquals(null, ToNumberFunction.toNumber("189", 8));
    assertEquals(null, ToNumberFunction.toNumber("GHI", 16));
    assertEquals(null, ToNumberFunction.toNumber("FF.8", 16));
  }

  @Test
  void testToNumberEdgeCases() {
    assertEquals(0L, ToNumberFunction.toNumber("0", 2));
    assertEquals(0L, ToNumberFunction.toNumber("0", 36));
    assertEquals(0.0, ToNumberFunction.toNumber("0.0", 10));
    assertEquals(0.0, ToNumberFunction.toNumber("0.000", 10));
  }

  @Test
  void testToNumberLargeNumbers() {
    assertEquals(
        (long) Integer.MAX_VALUE, ToNumberFunction.toNumber(String.valueOf(Integer.MAX_VALUE), 10));
    assertEquals(
        (long) Integer.MIN_VALUE, ToNumberFunction.toNumber(String.valueOf(Integer.MIN_VALUE), 10));
  }

  @Test
  void testToNumberCaseInsensitivity() {
    assertEquals(255L, ToNumberFunction.toNumber("ff", 16));
    assertEquals(255L, ToNumberFunction.toNumber("FF", 16));
    assertEquals(255L, ToNumberFunction.toNumber("fF", 16));
    assertEquals(171L, ToNumberFunction.toNumber("ab", 16));
    assertEquals(171L, ToNumberFunction.toNumber("AB", 16));
  }
}
