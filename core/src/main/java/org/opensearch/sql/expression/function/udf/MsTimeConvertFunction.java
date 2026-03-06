/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PPL mstime() conversion function. Converts {@code [MM:]SS.SSS} format to seconds
 * (SPL-compatible). The minutes portion is optional.
 */
public class MsTimeConvertFunction extends BaseConversionUDF {

  public static final MsTimeConvertFunction INSTANCE = new MsTimeConvertFunction();

  // Matches optional MM: prefix, required SS, optional .SSS
  private static final Pattern MSTIME_PATTERN =
      Pattern.compile("^(?:(\\d{1,2}):)?(\\d{1,2})(?:\\.(\\d{1,3}))?$");

  public MsTimeConvertFunction() {
    super(MsTimeConvertFunction.class);
  }

  public static Object convert(Object value) {
    return INSTANCE.convertValue(value);
  }

  @Override
  protected Object applyConversion(String preprocessedValue) {
    Double existingSeconds = tryParseDouble(preprocessedValue);
    if (existingSeconds != null) {
      return existingSeconds;
    }

    Matcher matcher = MSTIME_PATTERN.matcher(preprocessedValue);
    if (!matcher.matches()) {
      return null;
    }

    try {
      int minutes = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 0;
      int seconds = Integer.parseInt(matcher.group(2));

      if (seconds >= 60) {
        return null;
      }

      double millis = 0.0;
      if (matcher.group(3) != null) {
        String milliStr = matcher.group(3);
        // Pad to 3 digits
        while (milliStr.length() < 3) {
          milliStr += "0";
        }
        millis = Double.parseDouble(milliStr.substring(0, 3)) / 1000.0;
      }

      return (double) (minutes * 60 + seconds) + millis;
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
