/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import java.math.BigInteger;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class ConvFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
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
    // Validate base ranges
    if (fromBase < 2 || fromBase > 36 || toBase < 2 || toBase > 36) {
      return null;
    }

    // Check for sign
    boolean negative = false;
    if (numStr.startsWith("-")) {
      negative = true;
      numStr = numStr.substring(1);
    }

    // Normalize input (e.g., remove extra whitespace, convert to uppercase)
    numStr = numStr.trim().toUpperCase();

    // Try parsing the input as a BigInteger of 'fromBase'
    BigInteger value;
    try {
      value = new BigInteger(numStr, fromBase);
    } catch (NumberFormatException e) {
      // If numStr contains invalid characters for fromBase
      return "0";
    }

    // Re-apply sign if needed
    if (negative) {
      value = value.negate();
    }

    // Convert to the target base; BigInteger's toString(...) yields lowercase above 9
    String result = value.toString(toBase);

    // Convert to uppercase to align with MySQL's behavior
    return result.toUpperCase();
  }
}
