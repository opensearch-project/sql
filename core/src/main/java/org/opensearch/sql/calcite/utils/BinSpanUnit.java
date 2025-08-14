/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Dedicated span unit enum for bin command only. This is completely separate from the SpanUnit used
 * by aggregation span to avoid shared infrastructure that could break customer queries.
 */
@Getter
@RequiredArgsConstructor
public enum BinSpanUnit {
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
  QUARTER("q"),
  Q("q"),
  YEAR("y"),
  Y("y");

  private final String name;
  private static final List<BinSpanUnit> BIN_SPAN_UNITS;

  static {
    ImmutableList.Builder<BinSpanUnit> builder = ImmutableList.builder();
    BIN_SPAN_UNITS = builder.add(BinSpanUnit.values()).build();
  }

  /** Util method to get bin span unit given the unit name. */
  public static BinSpanUnit of(String unit) {
    if (unit == null || unit.isEmpty()) {
      return NONE;
    }

    // Handle case-sensitive distinction for minutes
    if (unit.equals("m")) {
      return m; // Minutes (lowercase)
    }

    // Try exact match first (case-sensitive)
    for (BinSpanUnit binUnit : BIN_SPAN_UNITS) {
      if (unit.equals(binUnit.name())) {
        return binUnit;
      }
    }

    // Try case-insensitive match for other units
    return BIN_SPAN_UNITS.stream()
        .filter(v -> unit.equalsIgnoreCase(v.name()))
        .findFirst()
        .orElse(UNKNOWN);
  }

  /** Get the string name of the bin span unit. */
  public static String getName(BinSpanUnit unit) {
    return unit.name;
  }
}
