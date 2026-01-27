/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Unit tests for conversion functions. */
public class ConversionFunctionsTest {

  // auto() Function Tests
  @Test
  public void testAutoConvertWithCommas() {
    assertEquals(1234.0, AutoConvertFunction.convert("1,234"));
    assertEquals(1234.56, AutoConvertFunction.convert("1,234.56"));
    assertEquals(1000000.0, AutoConvertFunction.convert("1,000,000"));
  }

  @Test
  public void testAutoConvertWithUnits() {
    assertEquals(123.0, AutoConvertFunction.convert("123 dollars"));
    assertEquals(45.67, AutoConvertFunction.convert("45.67 kg"));
    assertEquals(100.0, AutoConvertFunction.convert("100ms"));
    assertEquals(2.0, AutoConvertFunction.convert("2,12.0 sec"));
  }

  @Test
  public void testAutoConvertWithMemorySizes() {
    assertEquals(100.0, AutoConvertFunction.convert("100k"));
    assertEquals(51200.0, AutoConvertFunction.convert("50m"));
    assertEquals(2097152.0, AutoConvertFunction.convert("2g"));
    assertEquals(100.0, AutoConvertFunction.convert("100"));
    assertEquals(-100.0, AutoConvertFunction.convert("-100k"));
  }

  @Test
  public void testAutoConvertCombined() {
    assertEquals(1.0, AutoConvertFunction.convert("1,234 dollars"));
    assertEquals(5.0, AutoConvertFunction.convert("5,678.90 USD"));
  }

  @Test
  public void testAutoConvertComplexCommaPatterns() {
    assertEquals(2.0, AutoConvertFunction.convert("2.000"));
    assertEquals(22324.0, AutoConvertFunction.convert("2232,4.000,000"));
    assertEquals(2232.0, AutoConvertFunction.convert("2232,4.000,000AAAAA"));
  }

  @Test
  public void testAutoConvertStringsStartingWithLetters() {
    assertNull(AutoConvertFunction.convert("AAAA2.000"));
    assertNull(AutoConvertFunction.convert("AAAA2.000,000"));
  }

  @Test
  public void testAutoConvertNullAndEmpty() {
    assertNull(AutoConvertFunction.convert((Object) null));
    assertNull(AutoConvertFunction.convert(""));
    assertNull(AutoConvertFunction.convert("   "));
  }

  @Test
  public void testAutoConvertInvalid() {
    assertNull(AutoConvertFunction.convert("abc"));
    assertNull(AutoConvertFunction.convert("no numbers here"));
  }

  @Test
  public void testAutoConvertWithSpacedMemoryUnits() {
    assertEquals(123.0, AutoConvertFunction.convert("123 K"));
    assertEquals(123.0, AutoConvertFunction.convert("123 M"));
    assertEquals(123.0, AutoConvertFunction.convert("123 G"));
    assertEquals(50.5, AutoConvertFunction.convert("50.5 m"));
  }

  // num() Function Tests
  @Test
  public void testNumConvert() {
    assertEquals(123.0, NumConvertFunction.convert("123"));
    assertEquals(123.45, NumConvertFunction.convert("123.45"));
    assertEquals(1234.0, NumConvertFunction.convert("1,234"));
    assertEquals(123.0, NumConvertFunction.convert("123 dollars"));
  }

  @Test
  public void testNumConvertWithUnits() {
    assertEquals(212.0, NumConvertFunction.convert("212 sec"));
    assertNull(NumConvertFunction.convert("no numbers"));
  }

  @Test
  public void testNumConvertWithCommasAndUnits() {
    assertEquals(212.04, NumConvertFunction.convert("212.04,54545 AAA"));
    assertEquals(2.0, NumConvertFunction.convert("   2,12.0 AAA"));
    assertNull(NumConvertFunction.convert("AAAA2,12.0 AAA"));
    assertEquals(345445.0, NumConvertFunction.convert("34,54,45"));
  }

  @Test
  public void testNumConvertWithSpacedMemoryUnits() {
    // num() extracts numbers from strings with spaced units
    assertEquals(123.0, NumConvertFunction.convert("123 K"));
    assertEquals(123.0, NumConvertFunction.convert("123 M"));
    assertEquals(123.0, NumConvertFunction.convert("123 G"));
    assertEquals(50.5, NumConvertFunction.convert("50.5 m"));
  }

  @Test
  public void testNumConvertNullAndEmpty() {
    assertNull(NumConvertFunction.convert(null));
    assertNull(NumConvertFunction.convert(""));
    assertNull(NumConvertFunction.convert("   "));
  }

  // rmcomma() Function Tests
  @Test
  public void testRmcommaConvert() {
    assertEquals(1234.0, RmcommaConvertFunction.convert("1,234"));
    assertEquals(1234567.89, RmcommaConvertFunction.convert("1,234,567.89"));
    assertEquals(1234.0, RmcommaConvertFunction.convert("1234"));
    assertNull(RmcommaConvertFunction.convert("abc,123"));
    assertNull(RmcommaConvertFunction.convert(""));
    assertNull(RmcommaConvertFunction.convert(null));
  }

  @Test
  public void testRmcommaConvertVariations() {
    assertNull(RmcommaConvertFunction.convert("abc"));
    assertNull(RmcommaConvertFunction.convert("AAA3454,45"));
  }

  @Test
  public void testRmcommaConvertWithSpacedMemoryUnits() {
    assertNull(RmcommaConvertFunction.convert("123 K"));
    assertNull(RmcommaConvertFunction.convert("123 M"));
    assertNull(RmcommaConvertFunction.convert("123 G"));
    assertNull(RmcommaConvertFunction.convert("50.5 m"));
  }

  // rmunit() Function Tests
  @Test
  public void testRmunitConvert() {
    assertNull(RmunitConvertFunction.convert("no numbers"));
  }

  @Test
  public void testRmunitConvertEdgeCases() {
    assertEquals(2.0, RmunitConvertFunction.convert("2.000 sec"));
    assertEquals(2.0, RmunitConvertFunction.convert("2\\ sec"));
    assertNull(RmunitConvertFunction.convert("AAAA2\\ sec"));
    assertEquals(2.0, RmunitConvertFunction.convert("   2.000,7878789\\ sec"));
    assertEquals(34.0, RmunitConvertFunction.convert("34,54,45"));
  }

  @Test
  public void testRmunitConvertWithSpacedMemoryUnits() {
    assertEquals(123.0, RmunitConvertFunction.convert("123 K"));
    assertEquals(123.0, RmunitConvertFunction.convert("123 M"));
    assertEquals(123.0, RmunitConvertFunction.convert("123 G"));
    assertEquals(50.5, RmunitConvertFunction.convert("50.5 m"));
  }

  @Test
  public void testRmunitConvertNullAndEmpty() {
    assertNull(RmunitConvertFunction.convert(null));
    assertNull(RmunitConvertFunction.convert(""));
    assertNull(RmunitConvertFunction.convert("   "));
  }

  // memk() Function Tests
  @Test
  public void testMemkConvert() {
    assertEquals(100.0, MemkConvertFunction.convert("100"));
    assertEquals(100.0, MemkConvertFunction.convert(100));
    assertEquals(100.5, MemkConvertFunction.convert("100.5"));

    assertEquals(100.0, MemkConvertFunction.convert("100k"));
    assertEquals(100.0, MemkConvertFunction.convert("100K"));

    assertEquals(51200.0, MemkConvertFunction.convert("50m"));
    assertEquals(51200.0, MemkConvertFunction.convert("50M"));
    assertEquals(102912.0, MemkConvertFunction.convert("100.5m"));

    assertEquals(2097152.0, MemkConvertFunction.convert("2g"));
    assertEquals(2097152.0, MemkConvertFunction.convert("2G"));
    assertEquals(1.5 * 1024 * 1024, MemkConvertFunction.convert("1.5g"));

    assertEquals(-100.0, MemkConvertFunction.convert("-100"));
    assertEquals(-51200.0, MemkConvertFunction.convert("-50m"));
    assertEquals(-2097152.0, MemkConvertFunction.convert("-2g"));
    assertEquals(-100.0, MemkConvertFunction.convert("-100k"));

    assertEquals(100.0, MemkConvertFunction.convert("+100"));
    assertEquals(51200.0, MemkConvertFunction.convert("+50m"));

    assertNull(MemkConvertFunction.convert("abc"));
    assertNull(MemkConvertFunction.convert("100x"));
    assertNull(MemkConvertFunction.convert("100 gb"));
    assertNull(MemkConvertFunction.convert(""));
    assertNull(MemkConvertFunction.convert(null));
    assertNull(MemkConvertFunction.convert("   "));

    assertNull(MemkConvertFunction.convert("100 k"));
    assertNull(MemkConvertFunction.convert("50 m"));
    assertNull(MemkConvertFunction.convert("2 g"));

    assertNull(MemkConvertFunction.convert("abc100m"));
    assertNull(MemkConvertFunction.convert("test50k"));
    assertNull(MemkConvertFunction.convert("memory2g"));
  }

  // Cross-Function Tests
  @Test
  public void testScientificNotation() {
    assertEquals(100000.0, NumConvertFunction.convert("1e5"));
    assertEquals(100000.0, AutoConvertFunction.convert("1e5"));
    assertEquals(1.23e-4, NumConvertFunction.convert("1.23e-4"));
    assertEquals(1.23e-4, AutoConvertFunction.convert("1.23e-4"));
    assertEquals(100000.0, NumConvertFunction.convert("1e5 meters"));
    assertEquals(100000.0, RmunitConvertFunction.convert("1e5 meters"));
  }

  @Test
  public void testSpecialValues() {
    assertNull(NumConvertFunction.convert("∞"));
    assertNull(AutoConvertFunction.convert("∞"));
    assertNull(NumConvertFunction.convert("Infinity"));
    assertNull(AutoConvertFunction.convert("Infinity"));
    assertNull(NumConvertFunction.convert("NaN"));
    assertNull(AutoConvertFunction.convert("NaN"));
  }

  @Test
  public void testNegativeNumbers() {
    assertEquals(-123.0, NumConvertFunction.convert("-123"));
    assertEquals(-123.45, AutoConvertFunction.convert("-123.45"));
    assertEquals(-1234.0, RmcommaConvertFunction.convert("-1,234"));
    assertEquals(-100.0, RmunitConvertFunction.convert("-100km"));
  }

  @Test
  public void testLeadingPlusSign() {
    assertEquals(123.0, NumConvertFunction.convert("+123"));
    assertEquals(123.45, AutoConvertFunction.convert("+123.45"));
    assertEquals(100.0, RmunitConvertFunction.convert("+100km"));
    assertEquals(1234.0, RmcommaConvertFunction.convert("+1,234"));
  }

  @Test
  public void testMalformedNumbers() {
    assertNull(NumConvertFunction.convert("1.2.3"));
    assertNull(AutoConvertFunction.convert("1.2.3"));
    assertEquals(1234.0, NumConvertFunction.convert("1,,234"));
    assertEquals(1234.0, AutoConvertFunction.convert("1,,234"));
  }
}
