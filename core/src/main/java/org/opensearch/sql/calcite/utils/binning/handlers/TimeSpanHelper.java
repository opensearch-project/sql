/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.BinTimeSpanUtils;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.calcite.utils.binning.SpanParser;

/** Helper for creating time-based span expressions. */
public class TimeSpanHelper {

  /** Creates time span expression with optional alignment. */
  public RexNode createTimeSpanExpression(
      String spanStr, RexNode fieldExpr, RexNode alignTimeValue, CalcitePlanContext context) {

    // Check if aligntime should be applied
    if (alignTimeValue != null && shouldApplyAligntime(spanStr)) {
      return createAlignedTimeSpan(spanStr, fieldExpr, alignTimeValue, context);
    }

    // No alignment - use standard time span
    return createStandardTimeSpan(spanStr, fieldExpr, context);
  }

  private boolean shouldApplyAligntime(String spanStr) {
    if (spanStr == null) return false;

    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    String timeUnit = SpanParser.extractTimeUnit(spanStr);

    if (timeUnit == null) return true; // Pure number, assume hours

    // Aligntime ignored for days, months, years
    String normalizedUnit = SpanParser.getNormalizedUnit(timeUnit);
    return !normalizedUnit.equals("d") && !normalizedUnit.equals("months");
  }

  private RexNode createAlignedTimeSpan(
      String spanStr, RexNode fieldExpr, RexNode alignTimeValue, CalcitePlanContext context) {

    if (!(alignTimeValue instanceof RexLiteral)) {
      return createStandardTimeSpan(spanStr, fieldExpr, context);
    }

    Object value = ((RexLiteral) alignTimeValue).getValue();
    String aligntimeStr = extractAlignTimeString(value);

    if (aligntimeStr == null) {
      return createStandardTimeSpan(spanStr, fieldExpr, context);
    }

    // Parse span parameters
    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    String timeUnit = SpanParser.extractTimeUnit(spanStr);
    int intervalValue;
    String normalizedUnit;

    if (timeUnit != null) {
      String valueStr = spanStr.substring(0, spanStr.length() - timeUnit.length());
      intervalValue = Integer.parseInt(valueStr);
      normalizedUnit = SpanParser.getNormalizedUnit(timeUnit);
    } else {
      intervalValue = Integer.parseInt(spanStr);
      normalizedUnit = "h";
    }

    // Extract modifier from alignment string
    String modifier = extractModifier(aligntimeStr);

    return BinTimeSpanUtils.createBinTimeSpanExpressionWithTimeModifier(
        fieldExpr, intervalValue, normalizedUnit, modifier, context);
  }

  private RexNode createStandardTimeSpan(
      String spanStr, RexNode fieldExpr, CalcitePlanContext context) {

    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    String timeUnit = SpanParser.extractTimeUnit(spanStr);

    if (timeUnit != null) {
      String valueStr = spanStr.substring(0, spanStr.length() - timeUnit.length());
      int value = Integer.parseInt(valueStr);
      String normalizedUnit = SpanParser.getNormalizedUnit(timeUnit);
      return BinTimeSpanUtils.createBinTimeSpanExpression(
          fieldExpr, value, normalizedUnit, 0, context);
    } else {
      // Assume hours if no unit
      int value = Integer.parseInt(spanStr);
      return BinTimeSpanUtils.createBinTimeSpanExpression(fieldExpr, value, "h", 0, context);
    }
  }

  private String extractAlignTimeString(Object value) {
    if (value instanceof org.apache.calcite.util.NlsString) {
      return ((org.apache.calcite.util.NlsString) value).getValue();
    } else if (value instanceof String) {
      return (String) value;
    }
    return null;
  }

  private String extractModifier(String aligntimeStr) {
    aligntimeStr = aligntimeStr.replace("\"", "").replace("'", "").trim();

    if (aligntimeStr.startsWith(BinConstants.ALIGNTIME_EPOCH_PREFIX)) {
      return aligntimeStr.substring(BinConstants.ALIGNTIME_EPOCH_PREFIX.length());
    } else if (aligntimeStr.startsWith(BinConstants.ALIGNTIME_TIME_MODIFIER_PREFIX)) {
      return aligntimeStr.substring(BinConstants.ALIGNTIME_TIME_MODIFIER_PREFIX.length());
    } else if (aligntimeStr.startsWith("@d")) {
      return aligntimeStr;
    } else if (aligntimeStr.matches("\\d+")) {
      return aligntimeStr;
    }

    return null;
  }
}
