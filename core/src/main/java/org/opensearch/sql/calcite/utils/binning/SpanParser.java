/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parser for span strings to determine type and extract parameters. */
public class SpanParser {

  private static final Pattern LOG_PATTERN = Pattern.compile("^(\\d*\\.?\\d*)?log(\\d+\\.?\\d*)$");

  // Time units in order of precedence (longest first to avoid partial matches)
  private static final String[] TIME_UNITS = {
    // Full words first
    "seconds",
    "second",
    "secs",
    "sec",
    "minutes",
    "minute",
    "mins",
    "min",
    "hours",
    "hour",
    "hrs",
    "hr",
    "days",
    "day",
    "months",
    "month",
    "mon",
    // Sub-second units
    "us",
    "ms",
    "cs",
    "ds",
    // Single letter units (must be last)
    "M",
    "s",
    "m",
    "h",
    "d"
  };

  /** Parses a span string and returns span information. */
  public static SpanInfo parse(String spanStr) {
    String lowerSpanStr = spanStr.toLowerCase().trim();

    // Special handling for common log spans
    switch (lowerSpanStr) {
      case "log10":
        return new SpanInfo(SpanType.LOG, 1.0, 10.0);
      case "log2":
        return new SpanInfo(SpanType.LOG, 1.0, 2.0);
      case "loge":
      case "ln":
        return new SpanInfo(SpanType.LOG, 1.0, Math.E);
    }

    // Check for logarithmic pattern
    Matcher logMatcher = LOG_PATTERN.matcher(lowerSpanStr);
    if (logMatcher.matches()) {
      return parseLogSpan(logMatcher);
    }

    // Check for time-based span
    String timeUnit = extractTimeUnit(spanStr);
    if (timeUnit != null) {
      return parseTimeSpan(spanStr, timeUnit);
    }

    // Numeric span (fallback)
    return parseNumericSpan(spanStr);
  }

  private static SpanInfo parseLogSpan(Matcher logMatcher) {
    String coeffStr = logMatcher.group(1);
    String baseStr = logMatcher.group(2);

    double coefficient =
        (coeffStr == null || coeffStr.isEmpty()) ? 1.0 : Double.parseDouble(coeffStr);
    double base = Double.parseDouble(baseStr);

    // Validate log span parameters
    if (base <= 1.0) {
      throw new IllegalArgumentException("Log base must be > 1.0, got: " + base);
    }
    if (coefficient <= 0.0) {
      throw new IllegalArgumentException(
          "Log coefficient must be > 0.0, got coefficient=" + coefficient + ", base=" + base);
    }

    return new SpanInfo(SpanType.LOG, coefficient, base);
  }

  private static SpanInfo parseTimeSpan(String spanStr, String timeUnit) {
    String valueStr = spanStr.substring(0, spanStr.length() - timeUnit.length());
    double value = Double.parseDouble(valueStr);
    return new SpanInfo(SpanType.TIME, value, timeUnit);
  }

  private static SpanInfo parseNumericSpan(String spanStr) {
    try {
      double value = Double.parseDouble(spanStr);
      return new SpanInfo(SpanType.NUMERIC, value, null);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid span format: " + spanStr);
    }
  }

  /** Extracts time unit from span string. */
  public static String extractTimeUnit(String spanStr) {
    for (String unit : TIME_UNITS) {
      if (unit.equals("M")) {
        // Case-sensitive check for months
        if (spanStr.endsWith("M")) {
          return unit;
        }
      } else {
        // Case-insensitive check for other units
        String lowerSpanStr = spanStr.toLowerCase();
        String lowerUnit = unit.toLowerCase();
        if (lowerSpanStr.endsWith(lowerUnit)) {
          int unitStartPos = lowerSpanStr.length() - lowerUnit.length();
          if (unitStartPos == 0 || !Character.isLetter(lowerSpanStr.charAt(unitStartPos - 1))) {
            return unit;
          }
        }
      }
    }
    return null;
  }
}
