/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import java.util.Comparator;

/**
 * Comparator for MAX operations where strings have higher precedence than numbers.
 */
public class MaxTypeComparator implements Comparator<Object> {

  public static final MaxTypeComparator INSTANCE = new MaxTypeComparator();

  private MaxTypeComparator() {}

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

    // For different types strings have higher precedence
    return aIsNumeric ? -1 : 1;
  }

  /**
   * Returns the maximum value according to MAX precedence rules (strings > numbers).
   */
  public static Object max(Object a, Object b) {
    return INSTANCE.compare(a, b) >= 0 ? a : b;
  }

  private static boolean isNumeric(Object obj) {
    return obj instanceof Number;
  }
}