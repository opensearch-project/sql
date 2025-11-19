/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.ArrayList;
import java.util.List;

/**
 * Core logic for `mvappend` and internal `append` function to collect elements from list of args.
 */
public class AppendCore {

  /**
   * Collect non-null elements from `args`. If an item is a list, it will collect non-null elements
   * of the list. See {@ref AppendFunctionImplTest} for detailed behavior.
   */
  public static Object collectElements(Object... args) {
    List<Object> elements = collectElementsToList(args);

    if (elements.isEmpty()) {
      return null;
    } else if (elements.size() == 1) {
      // return the element in case of single element
      return elements.get(0);
    } else {
      return elements;
    }
  }

  /**
   * Collect non-null elements from `args`. If an item is a list, it will collect non-null elements.
   */
  public static List<Object> collectElementsToList(Object... args) {
    List<Object> elements = new ArrayList<>();

    for (Object arg : args) {
      if (arg == null) {
        continue;
      } else if (arg instanceof List) {
        addListElements((List<?>) arg, elements);
      } else {
        elements.add(arg);
      }
    }

    return elements;
  }

  private static void addListElements(List<?> list, List<Object> elements) {
    for (Object item : list) {
      if (item != null) {
        elements.add(item);
      }
    }
  }
}
