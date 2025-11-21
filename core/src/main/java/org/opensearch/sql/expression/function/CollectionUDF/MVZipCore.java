/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.ArrayList;
import java.util.List;

/** Core logic for `mvzip` command to combine two multivalue fields pairwise */
public class MVZipCore {

  /**
   * Combines values from two multivalue fields pairwise with a delimiter.
   *
   * <p>This function zips together two fields by combining the first value of left with the first
   * value of right, the second with the second, and so on, up to the length of the shorter field.
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li>Returns null if either left or right is null
   *   <li>Treats scalar values as single-element arrays
   *   <li>Stops at the length of the shorter field (like Python's zip)
   *   <li>Uses the provided delimiter to join values (default: ",")
   * </ul>
   *
   * @param left The left multivalue field or scalar value
   * @param right The right multivalue field or scalar value
   * @param delimiter The delimiter to use for joining values
   * @return A list of combined values, or null if either input is null
   */
  public static List<Object> zipElements(Object left, Object right, String delimiter) {
    // Return null if either field is null
    if (left == null || right == null) {
      return null;
    }

    // Convert inputs to lists (treating scalars as single-element arrays)
    List<?> leftList = toList(left);
    List<?> rightList = toList(right);

    // Create result list
    List<Object> result = new ArrayList<>();

    // Zip up to the shorter length
    int minLength = Math.min(leftList.size(), rightList.size());
    for (int i = 0; i < minLength; i++) {
      Object leftValue = leftList.get(i);
      Object rightValue = rightList.get(i);

      // Combine the values with the delimiter
      String combined = leftValue + delimiter + rightValue;
      result.add(combined);
    }

    return result.isEmpty() ? null : result;
  }

  /**
   * Converts an object to a list. If the object is already a list, returns it as-is. Otherwise,
   * wraps the object in a single-element list.
   */
  private static List<?> toList(Object obj) {
    if (obj instanceof List) {
      return (List<?>) obj;
    } else {
      return List.of(obj);
    }
  }
}
