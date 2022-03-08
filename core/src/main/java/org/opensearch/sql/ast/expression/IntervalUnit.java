/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum  IntervalUnit {
  UNKNOWN,

  MICROSECOND,
  SECOND,
  MINUTE,
  HOUR,
  DAY,
  WEEK,
  MONTH,
  QUARTER,
  YEAR,
  SECOND_MICROSECOND,
  MINUTE_MICROSECOND,
  MINUTE_SECOND,
  HOUR_MICROSECOND,
  HOUR_SECOND,
  HOUR_MINUTE,
  DAY_MICROSECOND,
  DAY_SECOND,
  DAY_MINUTE,
  DAY_HOUR,
  YEAR_MONTH;

  private static final List<IntervalUnit> INTERVAL_UNITS;

  static {
    ImmutableList.Builder<IntervalUnit> builder = new ImmutableList.Builder<>();
    INTERVAL_UNITS = builder.add(IntervalUnit.values()).build();
  }

  /**
   * Util method to get interval unit given the unit name.
   */
  public static IntervalUnit of(String unit) {
    return INTERVAL_UNITS.stream()
        .filter(v -> unit.equalsIgnoreCase(v.name()))
        .findFirst()
        .orElse(IntervalUnit.UNKNOWN);
  }
}
