/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parser for span strings to determine type and extract parameters. */
public class SpanParser {

  private static final Pattern LOG_PATTERN = Pattern.compile("^(\\d*\\.?\\d*)?log(\\d+\\.?\\d*)$");

  // Time units grouped by length for efficient lookup
  private static final Map<Integer, List<String>> TIME_UNITS_BY_LENGTH = new HashMap<>();

  // Map for normalizing time units to standard forms
  private static final Map<String, String> NORMALIZED_UNITS = new HashMap<>();

  static {
    // Define normalized units mapping (all variations map to standard form)
    String[][] unitMappings = {
      // Seconds variations
      {"seconds", "s"},
      {"second", "s"},
      {"secs", "s"},
      {"sec", "s"},
      {"s", "s"},
      // Minutes variations
      {"minutes", "m"},
      {"minute", "m"},
      {"mins", "m"},
      {"min", "m"},
      {"m", "m"},
      // Hours variations
      {"hours", "h"},
      {"hour", "h"},
      {"hrs", "h"},
      {"hr", "h"},
      {"h", "h"},
      // Days variations
      {"days", "d"},
      {"day", "d"},
      {"d", "d"},
      // Months variations
      {"months", "months"},
      {"month", "months"},
      {"mon", "months"},
      // Milliseconds
      {"ms", "ms"},
      // Microseconds
      {"us", "us"},
      // Centiseconds
      {"cs", "cs"},
      // Deciseconds
      {"ds", "ds"}
    };

    // Build normalized units map
    for (String[] mapping : unitMappings) {
      String normalized = mapping[mapping.length - 1];
      for (String variant : mapping) {
        NORMALIZED_UNITS.put(variant, normalized);
      }
    }

    // Initialize time units grouped by their length
    for (String unit : NORMALIZED_UNITS.keySet()) {
      TIME_UNITS_BY_LENGTH
          .computeIfAbsent(unit.length(), k -> new java.util.ArrayList<>())
          .add(unit);
    }

    // Sort lists by length in descending order for longest match first
    for (List<String> units : TIME_UNITS_BY_LENGTH.values()) {
      units.sort((a, b) -> Integer.compare(b.length(), a.length()));
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
    // Try to match units starting from longest possible length
    int spanLength = spanStr.length();

    // Check each possible unit length from longest to shortest
    for (int len = Math.min(7, spanLength); len > 0; len--) {
      List<String> unitsOfLength = TIME_UNITS_BY_LENGTH.get(len);
      if (unitsOfLength == null) continue;

      for (String unit : unitsOfLength) {
        // Case-insensitive check for all units
        String lowerSpanStr = spanStr.toLowerCase();
        String lowerUnit = unit.toLowerCase();
        if (lowerSpanStr.endsWith(lowerUnit)) {
          int unitStartPos = lowerSpanStr.length() - lowerUnit.length();
          if (unitStartPos == 0 || !Character.isLetter(lowerSpanStr.charAt(unitStartPos - 1))) {
            return unit; // Return original unit to preserve length for substring operations
          }
        }
      }
    }
    return null;
  }

  /** Returns the normalized form of a time unit. */
  public static String getNormalizedUnit(String unit) {
    return NORMALIZED_UNITS.getOrDefault(unit, unit);
  }
}
