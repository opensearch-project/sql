/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import java.util.Comparator;

/** Comparator for MIN operations where numbers have higher precedence than strings. */
public class MinTypeComparator implements Comparator<Object> {

  public static final MinTypeComparator INSTANCE = new MinTypeComparator();

  private MinTypeComparator() {}

  @Override
  public int compare(Object a, Object b) {
    boolean aIsNumeric = isNumeric(a);
    boolean bIsNumeric = isNumeric(b);

    // For same types compare directly
    if (aIsNumeric == bIsNumeric) {
      if (aIsNumeric) {
        return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
      } else {
        return a.toString().compareTo(b.toString());
      }
    }

    // For different types numbers have higher precedence
    return aIsNumeric ? -1 : 1;
  }

  /** Returns the minimum value according to MIN precedence rules (numbers > strings). */
  public static Object min(Object a, Object b) {
    return INSTANCE.compare(a, b) <= 0 ? a : b;
  }

  private static boolean isNumeric(Object obj) {
    return obj instanceof Number;
  }
}
