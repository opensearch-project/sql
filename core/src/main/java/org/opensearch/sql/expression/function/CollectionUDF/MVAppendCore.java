/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.ArrayList;
import java.util.List;

/** Core logic for `mvappend` command to collect elements from list of args */
public class MVAppendCore {
  public static List<Object> collectElements(Object... args) {
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

    return elements.isEmpty() ? null : elements;
  }

  private static void addListElements(List<?> list, List<Object> elements) {
    for (Object item : list) {
      if (item != null) {
        elements.add(item);
      }
    }
  }
}
