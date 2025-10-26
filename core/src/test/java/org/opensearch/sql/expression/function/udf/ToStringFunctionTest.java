/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.Locale;
import org.junit.jupiter.api.Test;

public class ToStringFunctionTest {

  private final ToStringFunction function = new ToStringFunction();

  @Test
  void testBigDecimalToStringDurationFormat() {
    BigDecimal num = new BigDecimal("3661"); // 1 hour 1 minute 1 second
    String result = ToStringFunction.toString(num, ToStringFunction.DURATION_FORMAT);
    assertEquals("01:01:01", result);
  }

  @Test
  void testBigDecimalToStringHexFormat() {
    BigDecimal num = new BigDecimal("255");
    String result = ToStringFunction.toString(num, ToStringFunction.HEX_FORMAT);
    assertEquals("ff", result);
  }

  @Test
  void testBigDecimalToStringCommasFormat() {
    Locale.setDefault(Locale.US); // Ensure predictable comma placement
    BigDecimal num = new BigDecimal("1234567.891");
    String result = ToStringFunction.toString(num, ToStringFunction.COMMAS_FORMAT);
    assertTrue(result.contains(","));
  }

  @Test
  void testBigDecimalToStringBinaryFormat() {
    BigDecimal num = new BigDecimal("10");
    String result = ToStringFunction.toString(num, ToStringFunction.BINARY_FORMAT);
    assertEquals("1010", result);
  }

  @Test
  void testBigDecimalToStringDefault() {
    BigDecimal num = new BigDecimal("123.45");
    assertEquals("123.45", ToStringFunction.toString(num, "unknown"));
  }

  @Test
  void testDoubleToStringDurationFormat() {
    double num = 3661.4;
    String result = ToStringFunction.toString(num, ToStringFunction.DURATION_FORMAT);
    assertEquals("01:01:01", result);
  }

  @Test
  void testDoubleToStringHexFormat() {
    double num = 10.5;
    String result = ToStringFunction.toString(num, ToStringFunction.HEX_FORMAT);
    assertTrue(result.equals("a"));
  }

  @Test
  void testDoubleToStringCommasFormat() {
    Locale.setDefault(Locale.US);
    double num = 12345.678;
    String result = ToStringFunction.toString(num, ToStringFunction.COMMAS_FORMAT);
    assertTrue(result.contains(","));
  }

  @Test
  void testDoubleToStringBinaryFormat() {
    double num = 10.0;
    String result = ToStringFunction.toString(num, ToStringFunction.BINARY_FORMAT);
    assertNotNull(result);
    assertFalse(result.isEmpty());
  }

  @Test
  void testDoubleToStringDefault() {
    assertEquals("10.5", ToStringFunction.toString(10.5, "unknown"));
  }

  @Test
  void testIntToStringDurationFormat() {
    int num = 3661;
    String result = ToStringFunction.toString(num, ToStringFunction.DURATION_FORMAT);
    assertEquals("01:01:01", result);
  }

  @Test
  void testIntToStringHexFormat() {
    assertEquals("ff", ToStringFunction.toString(255, ToStringFunction.HEX_FORMAT));
  }

  @Test
  void testIntToStringCommasFormat() {
    Locale.setDefault(Locale.US);
    String result = ToStringFunction.toString(1234567, ToStringFunction.COMMAS_FORMAT);
    assertTrue(result.contains(","));
  }

  @Test
  void testIntToStringBinaryFormat() {
    assertEquals("1010", ToStringFunction.toString(10, ToStringFunction.BINARY_FORMAT));
  }

  @Test
  void testIntToStringDefault() {
    assertEquals("123", ToStringFunction.toString(123, "unknown"));
  }

  @Test
  void testStringNumericToStringIntFormat() {
    String result = ToStringFunction.toString("42", ToStringFunction.HEX_FORMAT);
    assertEquals("2a", result);
  }

  @Test
  void testStringNumericToStringDoubleFormat() {
    String result = ToStringFunction.toString("42.5", ToStringFunction.COMMAS_FORMAT);
    assertTrue(result.contains("42"));
  }

  @Test
  void testStringLargeNumberAsDouble() {
    String largeNum = "1234567890123";
    String result = ToStringFunction.toString(largeNum, ToStringFunction.BINARY_FORMAT);
    assertNotNull(result);
  }
}
