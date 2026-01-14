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
  public void testRmcommaConvert() {
    assertEquals(1234.0, ConversionUtils.rmcommaConvert("1,234"));
    assertEquals(1234567.89, ConversionUtils.rmcommaConvert("1,234,567.89"));
    assertEquals(1234.0, ConversionUtils.rmcommaConvert("1234"));
    assertNull(ConversionUtils.rmcommaConvert("abc,123"));
    assertNull(ConversionUtils.rmcommaConvert(""));
    assertNull(ConversionUtils.rmcommaConvert(null));
  }

  @Test
  public void testAutoConvertWithCommas() {
    assertEquals(1234.0, ConversionUtils.autoConvert("1,234"));
    assertEquals(1234.56, ConversionUtils.autoConvert("1,234.56"));
    assertEquals(1000000.0, ConversionUtils.autoConvert("1,000,000"));
  }

  @Test
  public void testAutoConvertWithUnits() {
    assertEquals(123.0, ConversionUtils.autoConvert("123 dollars"));
    assertEquals(45.67, ConversionUtils.autoConvert("45.67 kg"));
    assertEquals(100.0, ConversionUtils.autoConvert("100ms"));
    assertEquals(2.0, ConversionUtils.autoConvert("2,12.0 sec"));
  }

  @Test
  public void testAutoConvertCombined() {
    assertEquals(1.0, ConversionUtils.autoConvert("1,234 dollars"));
    assertEquals(5.0, ConversionUtils.autoConvert("5,678.90 USD"));
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
    assertEquals(123.0, ConversionUtils.numConvert("123"));
    assertEquals(123.45, ConversionUtils.numConvert("123.45"));
    assertEquals(1234.0, ConversionUtils.numConvert("1,234"));
    assertEquals(123.0, ConversionUtils.numConvert("123 dollars"));
  }

  @Test
  public void testNumConvertWithUnits() {
    assertEquals(212.0, ConversionUtils.numConvert("212 sec"));
    assertEquals(45.67, ConversionUtils.numConvert("45.67 kg"));
    assertEquals(100.0, ConversionUtils.numConvert("100ms"));
    assertNull(ConversionUtils.numConvert("no numbers"));
  }

  @Test
  public void testNumConvertWithCommasAndUnits() {
    assertEquals(212.04, ConversionUtils.numConvert("212.04,54545 AAA"));
    assertEquals(2.0, ConversionUtils.numConvert("2,12.0 sec"));
    assertEquals(2.0, ConversionUtils.numConvert("   2,12.0 AAA"));
    assertNull(ConversionUtils.numConvert("AAAA2,12.0 AAA"));
    assertEquals(345445.0, ConversionUtils.numConvert("34,54,45"));
  }

  @Test
  public void testAutoConvertStringsStartingWithLetters() {
    assertNull(ConversionUtils.autoConvert("AAAA2.000"));
    assertNull(ConversionUtils.autoConvert("AAAA2.000,000"));
  }

  @Test
  public void testAutoConvertComplexCommaPatterns() {
    assertEquals(2.0, ConversionUtils.autoConvert("2.000"));
    assertEquals(22324.0, ConversionUtils.autoConvert("2232,4.000,000"));
    assertEquals(2232.0, ConversionUtils.autoConvert("2232,4.000,000AAAAA"));
  }

  @Test
  public void testRmcommaConvertVariations() {
    assertEquals(1234.0, ConversionUtils.rmcommaConvert("1,234"));
    assertEquals(1234.56, ConversionUtils.rmcommaConvert("1,234.56"));
    assertEquals(345445.0, ConversionUtils.rmcommaConvert("34,54,45"));
    assertNull(ConversionUtils.rmcommaConvert("abc"));
    assertNull(ConversionUtils.rmcommaConvert("AAA3454,45"));
  }

  @Test
  public void testRmunitConvert() {
    assertEquals(123.0, ConversionUtils.rmunitConvert("123 dollars"));
    assertEquals(45.67, ConversionUtils.rmunitConvert("45.67 kg"));
    assertNull(ConversionUtils.rmunitConvert("no numbers"));
  }

  @Test
  public void testRmunitConvertEdgeCases() {
    assertEquals(2.0, ConversionUtils.rmunitConvert("2.000 sec"));
    assertEquals(2.0, ConversionUtils.rmunitConvert("2\\ sec"));
    assertNull(ConversionUtils.rmunitConvert("AAAA2\\ sec"));
    assertEquals(2.0, ConversionUtils.rmunitConvert("   2.000,7878789\\ sec"));
    assertEquals(34.0, ConversionUtils.rmunitConvert("34,54,45"));
  }

  @Test
  public void testMalformedNumbers() {
    // Multiple decimal points should be rejected
    assertNull(ConversionUtils.numConvert("1.2.3"));
    assertNull(ConversionUtils.autoConvert("1.2.3"));

    // Multiple consecutive commas - just remove all commas and parse
    assertEquals(1234.0, ConversionUtils.numConvert("1,,234"));
    assertEquals(1234.0, ConversionUtils.autoConvert("1,,234"));
  }

  @Test
  public void testScientificNotation() {
    assertEquals(100000.0, ConversionUtils.numConvert("1e5"));
    assertEquals(100000.0, ConversionUtils.autoConvert("1e5"));
    assertEquals(1.23e-4, ConversionUtils.numConvert("1.23e-4"));
    assertEquals(1.23e-4, ConversionUtils.autoConvert("1.23e-4"));
    assertEquals(100000.0, ConversionUtils.numConvert("1e5 meters"));
    assertEquals(100000.0, ConversionUtils.rmunitConvert("1e5 meters"));
  }

  @Test
  public void testSpecialValues() {
    assertNull(ConversionUtils.numConvert("∞"));
    assertNull(ConversionUtils.autoConvert("∞"));
    assertNull(ConversionUtils.numConvert("Infinity"));
    assertNull(ConversionUtils.autoConvert("Infinity"));
    assertEquals(Double.NaN, ConversionUtils.numConvert("NaN"));
    assertEquals(Double.NaN, ConversionUtils.autoConvert("NaN"));
  }

  @Test
  public void testNegativeNumbers() {
    assertEquals(-123.0, ConversionUtils.numConvert("-123"));
    assertEquals(-123.45, ConversionUtils.autoConvert("-123.45"));
    assertEquals(-1234.0, ConversionUtils.rmcommaConvert("-1,234"));
    assertEquals(-100.0, ConversionUtils.rmunitConvert("-100km"));
  }

  @Test
  public void testLeadingPlusSign() {
    assertEquals(123.0, ConversionUtils.numConvert("+123"));
    assertEquals(123.45, ConversionUtils.autoConvert("+123.45"));
    assertEquals(100.0, ConversionUtils.rmunitConvert("+100km"));
  }
}
