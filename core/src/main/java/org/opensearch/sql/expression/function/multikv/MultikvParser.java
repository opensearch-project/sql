/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.multikv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Shared parsing logic for the {@code multikv} command UDFs.
 *
 * <p>{@link #parse} turns the table text of a single event into a list of serialized records, one
 * per table data row. Each record encodes the row's cells as {@code name<KV>value} pairs joined by
 * {@code <FS>}, where the column name comes from the detected header (or positional {@code
 * Column_N} naming under {@code noheader}). {@link #extract} looks a single column value up out of
 * one serialized record.
 *
 * <p>Columns are split on runs of whitespace. Richer aligned-offset handling is a later
 * enhancement.
 */
public final class MultikvParser {

  /** Field separator between cells inside a serialized record. */
  public static final String FS = "\u001f";

  /** Key/value separator between a column name and its value. */
  public static final String KV = "\u0002";

  private static final Pattern WS = Pattern.compile("\\s+");
  private static final Pattern LINE = Pattern.compile("\\R");
  private static final Pattern FS_SPLIT = Pattern.compile(Pattern.quote(FS));

  private MultikvParser() {}

  /**
   * Parse the table text into serialized per-row records.
   *
   * @param raw the table text (typically the {@code _raw} field)
   * @param forceHeader 1-based header line to force, or a value &lt;= 0 for auto-detection
   * @param noHeader when true, columns are named positionally ({@code Column_1}, ...)
   * @param filterTerms keep only rows containing at least one term; empty means keep all
   */
  public static List<String> parse(
      String raw, int forceHeader, boolean noHeader, List<String> filterTerms) {
    if (raw == null) {
      return Collections.emptyList();
    }
    List<String> lines = new ArrayList<>();
    for (String l : LINE.split(raw)) {
      if (!l.trim().isEmpty()) {
        lines.add(l);
      }
    }
    if (lines.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> header;
    int dataStart;
    if (noHeader) {
      header = null;
      dataStart = 0;
    } else if (forceHeader > 0) {
      int hIdx = Math.min(forceHeader - 1, lines.size() - 1);
      header = splitCols(lines.get(hIdx));
      dataStart = hIdx + 1;
    } else {
      header = splitCols(lines.get(0));
      dataStart = 1;
    }

    boolean hasFilter = filterTerms != null && !filterTerms.isEmpty();
    List<String> out = new ArrayList<>();
    for (int i = dataStart; i < lines.size(); i++) {
      String line = lines.get(i);
      if (hasFilter && !matchesFilter(line, filterTerms)) {
        continue;
      }
      out.add(serialize(header, splitCols(line)));
    }
    return out;
  }

  /** Extract the value for {@code col} out of one serialized record, or null when absent. */
  public static String extract(String record, String col) {
    if (record == null || col == null) {
      return null;
    }
    for (String pair : FS_SPLIT.split(record, -1)) {
      int idx = pair.indexOf(KV);
      if (idx < 0) {
        continue;
      }
      if (pair.substring(0, idx).equals(col)) {
        return pair.substring(idx + 1);
      }
    }
    return null;
  }

  private static boolean matchesFilter(String line, List<String> filterTerms) {
    for (String t : filterTerms) {
      if (t != null && !t.isEmpty() && line.contains(t)) {
        return true;
      }
    }
    return false;
  }

  private static List<String> splitCols(String line) {
    String t = line.trim();
    if (t.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.asList(WS.split(t));
  }

  private static String serialize(List<String> header, List<String> cells) {
    int n = (header != null) ? Math.max(header.size(), cells.size()) : cells.size();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) {
      String name = (header != null && i < header.size()) ? header.get(i) : ("Column_" + (i + 1));
      String val = (i < cells.size()) ? cells.get(i) : "";
      if (sb.length() > 0) {
        sb.append(FS);
      }
      sb.append(name).append(KV).append(val);
    }
    return sb.toString();
  }
}
