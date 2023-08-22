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
  SECOND("s"),
  S("s"),
  MINUTE("m"),
  m("m"),
  HOUR("h"),
  H("h"),
  DAY("d"),
  D("d"),
  WEEK("w"),
  W("w"),
  MONTH("M"),
  M("M"),
  QUARTER("q"),
  Q("q"),
  YEAR("y"),
  Y("y");

  private final String name;
  private static final List<SpanUnit> SPAN_UNITS;

  static {
    ImmutableList.Builder<SpanUnit> builder = ImmutableList.builder();
    SPAN_UNITS = builder.add(SpanUnit.values()).build();
  }

  /** Util method to get span unit given the unit name. */
  public static SpanUnit of(String unit) {
    switch (unit) {
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
