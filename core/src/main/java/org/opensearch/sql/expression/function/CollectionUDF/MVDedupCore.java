/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Core logic for `mvdedup` command to remove duplicate values from a multivalue field */
public class MVDedupCore {

  /**
   * Remove duplicate elements from the input array while preserving order. Null input returns null.
   * Empty array returns empty array. See {@ref MVDedupFunctionImplTest} for detailed behavior.
   *
   * @param array input array (can be null)
   * @return array with duplicates removed, or null if input is null
   */
  public static List<Object> removeDuplicates(Object array) {
    if (array == null) {
      return null;
    }

    if (!(array instanceof List)) {
      // If not a list, wrap it in a list
      List<Object> result = new ArrayList<>();
      result.add(array);
      return result;
    }

    List<?> inputList = (List<?>) array;
    if (inputList.isEmpty()) {
      return new ArrayList<>();
    }

    // Use LinkedHashSet to preserve insertion order while removing duplicates
    Set<Object> seen = new LinkedHashSet<>();
    for (Object item : inputList) {
      if (item != null) {
        seen.add(item);
      }
    }

    return new ArrayList<>(seen);
  }
}
