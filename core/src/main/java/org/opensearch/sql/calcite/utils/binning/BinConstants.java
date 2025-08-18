/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

/** Constants used across bin operations. */
public final class BinConstants {

  private BinConstants() {
    // Private constructor to prevent instantiation
  }

  // Formatting constants
  public static final String DASH_SEPARATOR = "-";
  public static final String OTHER_CATEGORY = "Other";
  public static final String INVALID_CATEGORY = "Invalid";

  // Bin count limits
  public static final int DEFAULT_BINS = 100;
  public static final int MIN_BINS = 2;
  public static final int MAX_BINS = 50000;

  // Time unit constants (milliseconds)
  public static final long MILLIS_PER_SECOND = 1000L;
  public static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
  public static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
  public static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

  // Sub-second conversions
  public static final long MICROS_PER_MILLI = 1000L;
  public static final long MILLIS_PER_CENTISECOND = 10L;
  public static final long MILLIS_PER_DECISECOND = 100L;

  // Historical reference points
  public static final int UNIX_EPOCH_YEAR = 1970;
  public static final String UNIX_EPOCH_DATE = "1970-01-01";

  // Alignment markers
  public static final String ALIGNTIME_EPOCH_PREFIX = "ALIGNTIME_EPOCH:";
  public static final String ALIGNTIME_TIME_MODIFIER_PREFIX = "ALIGNTIME_TIME_MODIFIER:";
}
