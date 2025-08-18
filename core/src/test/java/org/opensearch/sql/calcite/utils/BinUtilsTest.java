/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.binning.SpanParser;
import org.opensearch.sql.calcite.utils.binning.SpanType;

public class BinUtilsTest {

  @Test
  public void testParseSpanStringWithNumericSpan() {
    var spanInfo = SpanParser.parse("1000");
    assertEquals(SpanType.NUMERIC, spanInfo.getType());
    assertEquals(1000.0, spanInfo.getValue(), 0.001);
  }

  @Test
  public void testParseSpanStringWithLogSpan() {
    var spanInfo = SpanParser.parse("log10");
    assertEquals(SpanType.LOG, spanInfo.getType());
    assertEquals(1.0, spanInfo.getCoefficient(), 0.001);
    assertEquals(10.0, spanInfo.getBase(), 0.001);
  }

  @Test
  public void testParseSpanStringWithCoefficientLogSpan() {
    var spanInfo = SpanParser.parse("2log10");
    assertEquals(SpanType.LOG, spanInfo.getType());
    assertEquals(2.0, spanInfo.getCoefficient(), 0.001);
    assertEquals(10.0, spanInfo.getBase(), 0.001);
  }

  @Test
  public void testParseSpanStringWithArbitraryBase() {
    var spanInfo = SpanParser.parse("log3");
    assertEquals(SpanType.LOG, spanInfo.getType());
    assertEquals(1.0, spanInfo.getCoefficient(), 0.001);
    assertEquals(3.0, spanInfo.getBase(), 0.001);
  }

  @Test
  public void testParseSpanStringWithCoefficientArbitraryBase() {
    var spanInfo = SpanParser.parse("1.5log3");
    assertEquals(SpanType.LOG, spanInfo.getType());
    assertEquals(1.5, spanInfo.getCoefficient(), 0.001);
    assertEquals(3.0, spanInfo.getBase(), 0.001);
  }

  @Test
  public void testParseSpanStringWithTimeUnits() {
    var spanInfo = SpanParser.parse("30seconds");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(30.0, spanInfo.getValue(), 0.001);
    assertEquals("seconds", spanInfo.getUnit());
  }

  @Test
  public void testParseSpanStringWithSubsecondUnits() {
    var spanInfo = SpanParser.parse("500ms");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(500.0, spanInfo.getValue(), 0.001);
    assertEquals("ms", spanInfo.getUnit());

    spanInfo = SpanParser.parse("100us");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(100.0, spanInfo.getValue(), 0.001);
    assertEquals("us", spanInfo.getUnit());

    spanInfo = SpanParser.parse("2ds");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(2.0, spanInfo.getValue(), 0.001);
    assertEquals("ds", spanInfo.getUnit());
  }

  @Test
  public void testParseSpanStringWithExtendedTimeUnits() {
    var spanInfo = SpanParser.parse("7days");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(7.0, spanInfo.getValue(), 0.001);
    assertEquals("days", spanInfo.getUnit());

    spanInfo = SpanParser.parse("4months");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(4.0, spanInfo.getValue(), 0.001);
    assertEquals("months", spanInfo.getUnit());

    spanInfo = SpanParser.parse("15minutes");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(15.0, spanInfo.getValue(), 0.001);
    assertEquals("minutes", spanInfo.getUnit());

    // Test specific case: 1mon
    spanInfo = SpanParser.parse("1mon");
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(1.0, spanInfo.getValue(), 0.001);
    assertEquals("mon", spanInfo.getUnit());
  }

  @Test
  public void testMonthUnitDetection() {
    // Specifically test that 1mon is detected as "mon" not "m"
    String result = SpanParser.extractTimeUnit("1mon");
    System.out.println("Result for '1mon': " + result);
    assertEquals("mon", result);
  }

  @Test
  public void testFullMonthParsingChain() {
    // Test the full parsing chain for 1mon
    var spanInfo = SpanParser.parse("1mon");
    System.out.println(
        "SpanInfo: type="
            + spanInfo.getType()
            + ", value="
            + spanInfo.getValue()
            + ", unit="
            + spanInfo.getUnit());
    assertEquals(SpanType.TIME, spanInfo.getType());
    assertEquals(1.0, spanInfo.getValue(), 0.001);
    assertEquals("mon", spanInfo.getUnit());
  }

  @Test
  public void testTimeUnitExtraction() {
    // Test longest match first (prevents "ds" from matching "seconds")
    assertEquals("seconds", SpanParser.extractTimeUnit("30seconds"));
    assertEquals("minutes", SpanParser.extractTimeUnit("15minutes"));
    assertEquals("hours", SpanParser.extractTimeUnit("2hours"));
    assertEquals("days", SpanParser.extractTimeUnit("7days"));
    assertEquals("months", SpanParser.extractTimeUnit("4months"));

    // Test subsecond units
    assertEquals("ms", SpanParser.extractTimeUnit("500ms"));
    assertEquals("us", SpanParser.extractTimeUnit("100us"));
    assertEquals("cs", SpanParser.extractTimeUnit("50cs"));
    assertEquals("ds", SpanParser.extractTimeUnit("2ds"));

    // Test single letter units
    assertEquals("s", SpanParser.extractTimeUnit("30s"));
    assertEquals("m", SpanParser.extractTimeUnit("15m"));
    assertEquals("h", SpanParser.extractTimeUnit("2h"));
    assertEquals("d", SpanParser.extractTimeUnit("7d"));
  }

  // Legacy method tests - these use the deprecated methods but should still work
  @Test
  public void testLegacyParseSpanString() {
    var spanInfo = BinUtils.parseSpanString("1000");
    assertEquals(SpanType.NUMERIC, spanInfo.getType());
    assertEquals(1000.0, spanInfo.getValue(), 0.001);
  }

  @Test
  public void testLegacyExtractTimeUnit() {
    assertEquals("seconds", BinUtils.extractTimeUnit("30seconds"));
    assertEquals("h", BinUtils.extractTimeUnit("2h"));
  }
}
