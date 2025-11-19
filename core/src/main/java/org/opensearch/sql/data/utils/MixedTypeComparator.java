/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import java.util.Comparator;

/** Comparator for mixed-type values. */
public class MixedTypeComparator implements Comparator<Object> {

  public static final MixedTypeComparator INSTANCE = new MixedTypeComparator();

  private MixedTypeComparator() {}

  @Override
  public int compare(Object a, Object b) {
    boolean aIsNumeric = isNumeric(a);
    boolean bIsNumeric = isNumeric(b);

    // For same types compare directly
    if (aIsNumeric == bIsNumeric) {
      if (aIsNumeric) {
        return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
      } else {
        return Integer.compare(a.toString().compareTo(b.toString()), 0);
      }
    }
    // For mixed types, strings are considered larger than numbers (non-numeric values are treated
    // as strings)
    return aIsNumeric ? -1 : 1;
  }

  private static boolean isNumeric(Object obj) {
    return obj instanceof Number;
  }
}
