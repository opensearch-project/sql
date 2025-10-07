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
public enum SpanUnit {
  UNKNOWN("unknown"),
  NONE(""),
  MILLISECOND("ms"),
  MS("ms"),
  SECONDS("s"),
  SECOND("s"),
  SECS("s"),
  SEC("s"),
  S("s"),
  MINUTES("m"),
  MINUTE("m"),
  MINS("m"),
  MIN("m"),
  m("m"),
  HOURS("h"),
  HOUR("h"),
  HRS("h"),
  HR("h"),
  H("h"),
  DAYS("d"),
  DAY("d"),
  D("d"),
  WEEKS("w"),
  WEEK("w"),
  W("w"),
  MONTH("M"),
  MONTHS("M"),
  MON("M"),
  M("M"),
  QUARTERS("q"),
  QUARTER("q"),
  QTRS("q"),
  QTR("q"),
  Q("q"),
  YEARS("y"),
  YEAR("y"),
  Y("y");

  private final String name;
  private static final List<SpanUnit> SPAN_UNITS;

  static {
    ImmutableList.Builder<SpanUnit> builder = ImmutableList.builder();
    SPAN_UNITS = builder.add(SpanUnit.values()).build();
  }

  /** Util method to check if the unit is time unit. */
  public static boolean isTimeUnit(SpanUnit unit) {
    return unit != UNKNOWN && unit != NONE;
  }

  /** Util method to get span unit given the unit name. */
  public static SpanUnit of(String unit) {
    switch (unit) {
      case null:
      case "":
        return NONE;
      case "M":
        return M;
      case "m":
        return m;
      default:
        return SPAN_UNITS.stream()
            .filter(v -> unit.equalsIgnoreCase(v.name()))
            .findFirst()
            .orElse(UNKNOWN);
    }
  }

  public static String getName(SpanUnit unit) {
    return unit.name;
  }
}
