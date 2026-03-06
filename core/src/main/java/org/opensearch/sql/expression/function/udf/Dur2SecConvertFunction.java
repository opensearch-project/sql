/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** PPL dur2sec() conversion function. Converts duration format {@code [D+]HH:MM:SS} to seconds */
public class Dur2SecConvertFunction extends BaseConversionUDF {

  public static final Dur2SecConvertFunction INSTANCE = new Dur2SecConvertFunction();

  // Matches [D+]HH:MM:SS — optional days prefix with + separator
  private static final Pattern DURATION_PATTERN =
      Pattern.compile("^(?:(\\d+)\\+)?(\\d{1,2}):(\\d{1,2}):(\\d{1,2})$");

  public Dur2SecConvertFunction() {
    super(Dur2SecConvertFunction.class);
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

    Matcher matcher = DURATION_PATTERN.matcher(preprocessedValue);
    if (!matcher.matches()) {
      return null;
    }

    try {
      int days = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 0;
      int hours = Integer.parseInt(matcher.group(2));
      int minutes = Integer.parseInt(matcher.group(3));
      int seconds = Integer.parseInt(matcher.group(4));

      if (hours >= 24 || minutes >= 60 || seconds >= 60) {
        return null;
      }

      return (double) (days * 86400 + hours * 3600 + minutes * 60 + seconds);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
