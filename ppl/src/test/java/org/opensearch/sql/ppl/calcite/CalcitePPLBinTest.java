/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.sql.calcite.utils.BinUtils;

public class CalcitePPLBinTest {

  @Test
  public void testSpanParsing() {
    var numericSpan = BinUtils.parseSpanString("1000");
    assertEquals(BinUtils.SpanType.NUMERIC, numericSpan.type);
    assertEquals(1000.0, numericSpan.value, 0.001);

    var logSpan = BinUtils.parseSpanString("log10");
    assertEquals(BinUtils.SpanType.LOG, logSpan.type);
    assertEquals(1.0, logSpan.coefficient, 0.001);
    assertEquals(10.0, logSpan.base, 0.001);

    var timeSpan = BinUtils.parseSpanString("30seconds");
    assertEquals(BinUtils.SpanType.TIME, timeSpan.type);
    assertEquals(30.0, timeSpan.value, 0.001);
    assertEquals("seconds", timeSpan.unit);
  }

  @Test
  public void testTimeUnits() {
    var usSpan = BinUtils.parseSpanString("100us");
    assertEquals("us", usSpan.unit);

    var msSpan = BinUtils.parseSpanString("500ms");
    assertEquals("ms", msSpan.unit);

    var daysSpan = BinUtils.parseSpanString("7days");
    assertEquals("days", daysSpan.unit);

    var monthsSpan = BinUtils.parseSpanString("4months");
    assertEquals("months", monthsSpan.unit);
  }

  @Test
  public void testTimeUnitExtraction() {
    assertEquals("seconds", BinUtils.extractTimeUnit("30seconds"));
    assertEquals("minutes", BinUtils.extractTimeUnit("15minutes"));
    assertEquals("ms", BinUtils.extractTimeUnit("500ms"));
    assertEquals("us", BinUtils.extractTimeUnit("100us"));
    assertEquals("s", BinUtils.extractTimeUnit("30s"));
    assertEquals("d", BinUtils.extractTimeUnit("7d"));
  }
}
