/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PPL dur2sec() conversion function.
 * Converts duration format [D+]HH:MM:SS to seconds.
 * Examples:
 * - "01:23:45" -> 5025 seconds (1*3600 + 23*60 + 45)
 * - "2+12:30:15" -> 217815 seconds (2*24*3600 + 12*3600 + 30*60 + 15)
 */
public class Dur2SecConvertFunction extends BaseConversionUDF {

  public static final Dur2SecConvertFunction INSTANCE = new Dur2SecConvertFunction();

  // Pattern to match [D+]HH:MM:SS format
  // Optional days followed by +, then HH:MM:SS
  private static final Pattern DURATION_PATTERN =
      Pattern.compile("^(?:(\\d+)\\+)?(\\d{1,2}):(\\d{1,2}):(\\d{1,2})$");

  public Dur2SecConvertFunction() {
    super(Dur2SecConvertFunction.class);
  }

  public static Object convert(Object value) {
    return INSTANCE.convertValueImpl(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    // First try to parse as a number (already in seconds)
    Double existingSeconds = tryParseDouble(preprocessedValue);
    if (existingSeconds != null) {
      return existingSeconds;
    }

    // Try to parse as [D+]HH:MM:SS format
    Matcher matcher = DURATION_PATTERN.matcher(preprocessedValue);
    if (matcher.matches()) {
      try {
        int days = 0;
        if (matcher.group(1) != null) {
          days = Integer.parseInt(matcher.group(1));
        }

        int hours = Integer.parseInt(matcher.group(2));
        int minutes = Integer.parseInt(matcher.group(3));
        int seconds = Integer.parseInt(matcher.group(4));

        // Validate time components are in proper ranges
        if (hours >= 24 || minutes >= 60 || seconds >= 60) {
          return null;
        }

        // Convert to total seconds
        int totalSeconds = days * 24 * 3600 + hours * 3600 + minutes * 60 + seconds;
        return (double) totalSeconds;

      } catch (NumberFormatException e) {
        return null;
      }
    }

    return null;
  }

  public Object convertValueImpl(Object value) {
    if (value instanceof Number) {
      // Already a number (seconds), return as double
      return ((Number) value).doubleValue();
    }

    String str = preprocessValue(value);
    if (str == null) {
      return null;
    }

    return applyConversion(str);
  }
}