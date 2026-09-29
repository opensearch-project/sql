/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Pivots the unpivoted rows produced by {@code CalciteRelNodeVisitor.visitTimewrap} into the
 * Splunk-style period columns timewrap returns.
 *
 * <p>The timewrap RelNode intentionally does NOT pivot — the set of period columns is only known
 * once all rows are read. Instead it emits rows of shape {@code [display_ts, value_col(s)...,
 * __base_offset__, __period__]} and signals this post-processing step via the thread-locals on
 * {@link CalcitePlanContext}. This helper performs the dynamic pivot so both execution engines (the
 * v2 {@code OpenSearchExecutionEngine} and the analytics-route {@code AnalyticsExecutionEngine})
 * produce identical output.
 */
public final class TimewrapPivot {

  private TimewrapPivot() {}

  /** Result of a pivot: the rebuilt columns and rows. */
  public record Result(List<Column> columns, List<ExprValue> values) {}

  /**
   * Returns true when the current query is a timewrap query whose results need pivoting. Reads the
   * {@link CalcitePlanContext} thread-locals set by {@code visitTimewrap}.
   */
  public static boolean isTimewrap() {
    return Boolean.TRUE.equals(CalcitePlanContext.stripNullColumns.get())
        && CalcitePlanContext.timewrapUnitName.get() != null;
  }

  /**
   * Pivots the unpivoted timewrap rows into period columns. If the input is empty or the
   * bookkeeping columns are absent, the input is returned unchanged.
   *
   * @param columns the unpivoted columns {@code [display_ts, value_col(s)..., __base_offset__,
   *     __period__]}
   * @param values the unpivoted rows
   * @param unitInfo the {@code visitTimewrap} unit descriptor
   *     ("spanValue|singular|plural|_before"); null means this is not a timewrap query and the
   *     input is returned unchanged
   * @param seriesMode the timewrap {@code series} mode (relative / short / exact); may be null
   */
  public static Result pivot(
      List<Column> columns, List<ExprValue> values, String unitInfo, String seriesMode) {
    if (unitInfo == null || values.isEmpty()) {
      return new Result(columns, values);
    }

    // Locate the bookkeeping and value columns. visitTimewrap always emits
    // [display_ts, value_col(s)..., __base_offset__, __period__], so column 0 is the timestamp,
    // __period__/__base_offset__ are bookkeeping, and every other column is a value column.
    int tsIdx = 0;
    int periodIdx = -1;
    int baseOffsetIdx = -1;
    List<Integer> valueIdxs = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      String name = columns.get(i).getName();
      if ("__period__".equals(name)) periodIdx = i;
      else if ("__base_offset__".equals(name)) baseOffsetIdx = i;
      else if (i > 0) valueIdxs.add(i);
    }
    if (periodIdx < 0 || baseOffsetIdx < 0) {
      return new Result(columns, values);
    }

    // Read __base_offset__ (constant across all rows).
    long baseOffset = 0;
    ExprValue boVal = values.getFirst().tupleValue().get("__base_offset__");
    if (boVal != null && !boVal.isNull()) {
      baseOffset = boVal.longValue();
    }

    // Collect distinct periods (sorted descending = oldest first in output) and precompute each
    // period's name once. The name depends only on (period, baseOffset, unitInfo, seriesMode), so
    // computing it inside the per-row loop would repeat the same split/parse/switch
    // O(rows x valueCols) times.
    Set<Long> periodSet = new TreeSet<>(Collections.reverseOrder());
    for (ExprValue row : values) {
      ExprValue pv = row.tupleValue().get("__period__");
      if (pv != null && !pv.isNull()) {
        periodSet.add(pv.longValue());
      }
    }
    List<Long> periods = new ArrayList<>(periodSet);
    Map<Long, String> periodNames = new HashMap<>();
    for (long period : periods) {
      periodNames.put(period, renameTimewrapPeriod(period, baseOffset, unitInfo, seriesMode));
    }

    // Value column names.
    List<String> valueColNames = new ArrayList<>();
    for (int vi : valueIdxs) {
      valueColNames.add(columns.get(vi).getName());
    }

    // Build output column names: [ts, val1_period1, val1_period2, ..., val2_period1, ...].
    // Splunk order: for each period, all value columns (oldest period first).
    List<String> outColNames = new ArrayList<>();
    outColNames.add(columns.get(tsIdx).getName());
    List<org.opensearch.sql.data.type.ExprType> outColTypes = new ArrayList<>();
    outColTypes.add(columns.get(tsIdx).getExprType());

    for (long period : periods) {
      for (int vi = 0; vi < valueColNames.size(); vi++) {
        outColNames.add(valueColNames.get(vi) + "_" + periodNames.get(period));
        outColTypes.add(columns.get(valueIdxs.get(vi)).getExprType());
      }
    }

    // Group rows by display_ts, pivot periods into columns. LinkedHashMap preserves the
    // ts-sorted insertion order Calcite produced.
    Map<String, Map<String, ExprValue>> pivoted = new LinkedHashMap<>();
    String tsColName = columns.get(tsIdx).getName();
    for (ExprValue row : values) {
      Map<String, ExprValue> tuple = row.tupleValue();
      String tsKey = tuple.get(tsColName).toString();
      long period = tuple.get("__period__").longValue();

      Map<String, ExprValue> outRow =
          pivoted.computeIfAbsent(
              tsKey,
              k -> {
                Map<String, ExprValue> r = new LinkedHashMap<>();
                r.put(outColNames.get(0), tuple.get(tsColName));
                // Initialize all period columns to null.
                for (int i = 1; i < outColNames.size(); i++) {
                  r.put(outColNames.get(i), ExprNullValue.of());
                }
                return r;
              });

      // Fill in the value for this period.
      String periodName = periodNames.get(period);
      for (int vi = 0; vi < valueColNames.size(); vi++) {
        String colName = valueColNames.get(vi) + "_" + periodName;
        ExprValue val = tuple.get(valueColNames.get(vi));
        if (val != null) {
          outRow.put(colName, val);
        }
      }
    }

    // Build output.
    List<Column> outColumns = new ArrayList<>();
    for (int i = 0; i < outColNames.size(); i++) {
      outColumns.add(new Column(outColNames.get(i), null, outColTypes.get(i)));
    }
    List<ExprValue> outValues = new ArrayList<>();
    for (Map<String, ExprValue> outRow : pivoted.values()) {
      outValues.add(ExprTupleValue.fromExprValueMap(outRow));
    }
    return new Result(outColumns, outValues);
  }

  /**
   * Generates a period name from a relative period number and base offset. Returns the suffix only
   * (no value prefix). E.g., "2days_before", "latest_day", "s2". unitInfo format:
   * "spanValue|singular|plural|_before".
   */
  private static String renameTimewrapPeriod(
      long relativePeriod, long baseOffset, String unitInfo, String seriesMode) {
    String[] parts = unitInfo.split("\\|", -1);
    if (parts.length < 4) return String.valueOf(relativePeriod);
    int spanValue = Integer.parseInt(parts[0]);
    String singular = parts[1];
    String plural = parts[2];

    long absolutePeriod = (baseOffset + relativePeriod - 1) * spanValue;

    String mode = seriesMode == null ? "relative" : seriesMode;

    return switch (mode) {
      // series=exact (+ time_format) is not yet implemented; it intentionally falls back to the
      // short "s<N>" naming. TODO: format the period start date with time_format.
      case "short", "exact" -> "s" + absolutePeriod;
      default -> {
        if (absolutePeriod == 0) {
          yield "latest_" + singular;
        } else if (absolutePeriod > 0) {
          String unit = absolutePeriod == 1 ? singular : plural;
          yield absolutePeriod + unit + "_before";
        } else {
          long absPeriod = Math.abs(absolutePeriod);
          String unit = absPeriod == 1 ? singular : plural;
          yield absPeriod + unit + "_after";
        }
      }
    };
  }
}
