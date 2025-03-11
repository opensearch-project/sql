/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;

/**
 * Convert number x from base a to base b<br>
 * The supported signature of floor function is<br>
 * (STRING, INTEGER, INTEGER) -> STRING<br>
 * (INTEGER, INTEGER, INTEGER) -> STRING
 */
public class ConvFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          String.format("CONV function requires exactly three arguments, but got %d", args.length));
    }

    Object number = args[0];
    Object fromBase = args[1];
    Object toBase = args[2];

    String numStr = number.toString();
    int fromBaseInt = Integer.parseInt(fromBase.toString());
    int toBaseInt = Integer.parseInt(toBase.toString());
    return conv(numStr, fromBaseInt, toBaseInt);
  }

  /**
   * Convert numStr from fromBase to toBase
   *
   * @param numStr the number to convert (case-insensitive for alphanumeric digits, may have a
   *     leading '-')
   * @param fromBase base of the input number (2 to 36)
   * @param toBase target base (2 to 36)
   * @return the converted number in the target base (uppercase), "0" if the input is invalid, or
   *     null if bases are out of range.
   */
  private static String conv(String numStr, int fromBase, int toBase) {
    return Long.toString(Long.parseLong(numStr, fromBase), toBase);
  }
}
