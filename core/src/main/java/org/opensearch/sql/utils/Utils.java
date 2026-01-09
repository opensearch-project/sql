/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public interface Utils {
  static <I> List<Pair<I, Integer>> zipWithIndex(List<I> input) {
    LinkedList<Pair<I, Integer>> result = new LinkedList<>();
    Iterator<I> iter = input.iterator();
    int index = 0;
    while (iter.hasNext()) {
      result.add(Pair.of(iter.next(), index++));
    }
    return result;
  }

  /**
   * Resolve the nested path from the field name.
   *
   * @param path the field name
   * @param fieldTypes the field types
   * @return the nested path if exists, otherwise null
   */
  static @Nullable String resolveNestedPath(String path, Map<String, ExprType> fieldTypes) {
    if (path == null || fieldTypes == null || fieldTypes.isEmpty()) {
      return null;
    }
    boolean found = false;
    String current = path;
    String parent = StringUtils.substringBeforeLast(current, ".");
    while (parent != null && !parent.equals(current)) {
      ExprType pathType = fieldTypes.get(parent);
      // Nested is mapped to ExprCoreType.ARRAY
      if (pathType == ExprCoreType.ARRAY) {
        found = true;
        break;
      }
      current = parent;
      parent = StringUtils.substringBeforeLast(current, ".");
    }
    if (found) {
      return parent;
    } else {
      return null;
    }
  }
}
