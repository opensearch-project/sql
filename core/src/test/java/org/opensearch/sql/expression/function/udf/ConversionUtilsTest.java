/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Unit tests for ConversionUtils. */
public class ConversionUtilsTest {

  @Test
  public void testAutoConvertBasicNumbers() {
    assertEquals(123L, ConversionUtils.autoConvert("123"));
    assertEquals(123.45, ConversionUtils.autoConvert("123.45"));
    assertEquals(0L, ConversionUtils.autoConvert("0"));
    assertEquals(-123L, ConversionUtils.autoConvert("-123"));
    assertEquals(42L, ConversionUtils.autoConvert("42"));
    assertEquals(3.14, ConversionUtils.autoConvert("3.14"));
  }

  @Test
  public void testAutoConvertWithCommas() {
    assertEquals(1234L, ConversionUtils.autoConvert("1,234"));
    assertEquals(1234.56, ConversionUtils.autoConvert("1,234.56"));
    assertEquals(1000000L, ConversionUtils.autoConvert("1,000,000"));
  }

  @Test
  public void testAutoConvertWithUnits() {
    assertEquals(123L, ConversionUtils.autoConvert("123 dollars"));
    assertEquals(45.67, ConversionUtils.autoConvert("45.67 kg"));
    assertEquals(100L, ConversionUtils.autoConvert("100ms"));
  }

  @Test
  public void testAutoConvertCombined() {
    assertEquals(1234L, ConversionUtils.autoConvert("1,234 dollars"));
    assertEquals(5678.90, ConversionUtils.autoConvert("5,678.90 USD"));
  }

  @Test
  public void testAutoConvertNullAndEmpty() {
    assertNull(ConversionUtils.autoConvert((Object) null));
    assertNull(ConversionUtils.autoConvert(""));
    assertNull(ConversionUtils.autoConvert("   "));
  }

  @Test
  public void testAutoConvertInvalid() {
    assertNull(ConversionUtils.autoConvert("abc"));
    assertNull(ConversionUtils.autoConvert("no numbers here"));
  }

  @Test
  public void testNumConvert() {
    assertEquals(123L, ConversionUtils.numConvert("123"));
    assertEquals(123.45, ConversionUtils.numConvert("123.45"));
    assertNull(ConversionUtils.numConvert("1,234"));
    assertNull(ConversionUtils.numConvert("123 dollars"));
  }

  @Test
  public void testRmcommaConvert() {
    assertEquals("1234", ConversionUtils.rmcommaConvert("1,234"));
    assertEquals("1234.56", ConversionUtils.rmcommaConvert("1,234.56"));
    assertEquals("abc", ConversionUtils.rmcommaConvert("abc"));
  }

  @Test
  public void testRmunitConvert() {
    assertEquals(123L, ConversionUtils.rmunitConvert("123 dollars"));
    assertEquals(45.67, ConversionUtils.rmunitConvert("45.67 kg"));
    assertNull(ConversionUtils.rmunitConvert("no numbers"));
  }
}
