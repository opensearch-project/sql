/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class for debugging operations. */
public class DebugUtils {

  private static void print(String format, Object... args) {
    System.out.println(String.format(format, args));
  }

  public static <T> T debug(T obj, String message) {
    print("### %s: %s (at %s)", message, stringify(obj), getCalledFrom(1));
    return obj;
  }

  public static <T> T debug(T obj) {
    print("### %s (at %s)", stringify(obj), getCalledFrom(1));
    return obj;
  }

  private static String getCalledFrom(int pos) {
    RuntimeException e = new RuntimeException();
    StackTraceElement item = e.getStackTrace()[pos + 1];
    return item.getClassName() + "." + item.getMethodName() + ":" + item.getLineNumber();
  }

  private static String stringify(Collection<?> items) {
    if (items == null) {
      return "null";
    }

    if (items.isEmpty()) {
      return "()";
    }

    String result = items.stream().map(i -> stringify(i)).collect(Collectors.joining(","));

    return "(" + result + ")";
  }

  private static String stringify(Map<?, ?> map) {
    if (map == null) {
      return "[[null]]";
    }

    if (map.isEmpty()) {
      return "[[EMPTY]]";
    }

    String result =
        map.entrySet().stream()
            .map(entry -> entry.getKey() + ": " + stringify(entry.getValue()))
            .collect(Collectors.joining(","));
    return "{" + result + "}";
  }

  private static String stringify(Object obj) {
    if (obj instanceof Collection) {
      return stringify((Collection) obj);
    } else if (obj instanceof Map) {
      return stringify((Map) obj);
    }
    return String.valueOf(obj);
  }
}
