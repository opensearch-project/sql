/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parser for span strings to determine type and extract parameters. */
public class SpanParser {

  private static final Pattern LOG_PATTERN = Pattern.compile("^(\\d*\\.?\\d*)?log(\\d+\\.?\\d*)$");

  // Map for normalizing time units to standard forms
  private static final Map<String, String> NORMALIZED_UNITS = new HashMap<>();

  // Direct lookup map for time units (lowercase -> original)
  private static final Map<String, String> UNIT_LOOKUP = new HashMap<>();

  static {
    // Define normalized units mapping using Map.ofEntries
    NORMALIZED_UNITS.putAll(
        Map.ofEntries(
            // Seconds variations
            Map.entry("seconds", "s"),
            Map.entry("second", "s"),
            Map.entry("secs", "s"),
            Map.entry("sec", "s"),
            Map.entry("s", "s"),
            // Minutes variations
            Map.entry("minutes", "m"),
            Map.entry("minute", "m"),
            Map.entry("mins", "m"),
            Map.entry("min", "m"),
            Map.entry("m", "m"),
            // Hours variations
            Map.entry("hours", "h"),
            Map.entry("hour", "h"),
            Map.entry("hrs", "h"),
            Map.entry("hr", "h"),
            Map.entry("h", "h"),
            // Days variations
            Map.entry("days", "d"),
            Map.entry("day", "d"),
            Map.entry("d", "d"),
            // Months variations
            Map.entry("months", "months"),
            Map.entry("month", "months"),
            Map.entry("mon", "months"),
            // Milliseconds
            Map.entry("ms", "ms"),
            // Microseconds
            Map.entry("us", "us"),
            // Centiseconds
            Map.entry("cs", "cs"),
            // Deciseconds
            Map.entry("ds", "ds")));

    // Build direct lookup map for efficient unit detection
    for (String unit : NORMALIZED_UNITS.keySet()) {
      UNIT_LOOKUP.put(unit.toLowerCase(), unit);
    }
  }

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

  /** Extracts time unit from span string (returns original matched unit, not normalized). */
  public static String extractTimeUnit(String spanStr) {
    String lowerSpanStr = spanStr.toLowerCase();
    String longestMatch = null;

    // Find the longest unit that matches as a suffix
    for (String unit : UNIT_LOOKUP.keySet()) {
      if (lowerSpanStr.endsWith(unit)) {
        // Ensure this is a word boundary (not part of a larger word)
        int unitStartPos = lowerSpanStr.length() - unit.length();
        if (unitStartPos == 0 || !Character.isLetter(lowerSpanStr.charAt(unitStartPos - 1))) {
          // Keep the longest match
          if (longestMatch == null || unit.length() > longestMatch.length()) {
            longestMatch = unit;
          }
        }
      }
    }

    return longestMatch != null ? UNIT_LOOKUP.get(longestMatch) : null;
  }

  /** Returns the normalized form of a time unit. */
  public static String getNormalizedUnit(String unit) {
    return NORMALIZED_UNITS.getOrDefault(unit, unit);
  }
}
