/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PPL mstime() conversion function.
 * Converts MM:SS.SSS format to seconds.
 * Example: "03:45.123" -> 225.123 seconds
 */
public class MsTimeConvertFunction extends BaseConversionUDF {

  public static final MsTimeConvertFunction INSTANCE = new MsTimeConvertFunction();

  // Pattern to match MM:SS.SSS or MM:SS format
  private static final Pattern MSTIME_PATTERN =
      Pattern.compile("^(\\d{1,2}):(\\d{1,2})(?:\\.(\\d{1,3}))?$");

  public MsTimeConvertFunction() {
    super(MsTimeConvertFunction.class);
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

    // Try to parse as MM:SS.SSS format
    Matcher matcher = MSTIME_PATTERN.matcher(preprocessedValue);
    if (matcher.matches()) {
      try {
        int minutes = Integer.parseInt(matcher.group(1));
        int seconds = Integer.parseInt(matcher.group(2));

        // Validate time components are in proper ranges
        if (seconds >= 60) {
          return null;
        }

        double milliseconds = 0.0;
        if (matcher.group(3) != null) {
          String milliStr = matcher.group(3);
          // Pad to 3 digits if necessary
          while (milliStr.length() < 3) {
            milliStr += "0";
          }
          // Truncate to 3 digits if longer
          if (milliStr.length() > 3) {
            milliStr = milliStr.substring(0, 3);
          }
          milliseconds = Double.parseDouble(milliStr) / 1000.0;
        }

        return (double) (minutes * 60 + seconds) + milliseconds;
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