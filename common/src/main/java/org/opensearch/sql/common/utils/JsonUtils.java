/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtils {
  /**
   * Utility method to build JSON string from multiple strings with single-quotes. This is just for
   * ease of read and maintain in tests. sjson("{", "'key': 'name'", "}") -> "{\n \"key\":
   * \"name\"\n}"
   *
   * @param lines lines using single-quote instead for double-quote
   * @return sting joined inputs and replaces single-quotes with double-quotes
   */
  public static String sjson(String... lines) {
    StringBuilder builder = new StringBuilder();
    for (String line : lines) {
      builder.append(replaceQuote(line));
      builder.append("\n");
    }
    return builder.toString();
  }

  private static String replaceQuote(String line) {
    return line.replace("'", "\"");
  }

  /**
   * Utility method to build multiline string from list of strings. Last line will also have new
   * line at the end.
   *
   * @param lines input lines
   * @return string contains lines
   */
  public static String lines(String... lines) {
    StringBuilder builder = new StringBuilder();
    for (String line : lines) {
      builder.append(line);
      builder.append("\n");
    }
    return builder.toString();
  }
}
