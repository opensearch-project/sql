/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;

public class LambdaUtils {
  /**
   * Convert a BigDecimal lambda result to the specified SQL target type.
   *
   * <p>If `candidate` is a BigDecimal, convert it to `Integer`, `Double`, or `Float` when
   * `targetType` is INTEGER, DOUBLE, or FLOAT respectively; otherwise return the BigDecimal.
   * If `candidate` is not a BigDecimal, return it unchanged.
   *
   * @param candidate the lambda-produced value to convert
   * @param targetType the desired SQL target type; supported conversions: INTEGER, DOUBLE, FLOAT
   * @return the converted value: an `Integer`, `Double`, `Float`, the original `BigDecimal`, or the original `candidate` if no conversion was performed
   */
  public static Object transferLambdaOutputToTargetType(Object candidate, SqlTypeName targetType) {
    if (candidate instanceof BigDecimal) {
      BigDecimal bd = (BigDecimal) candidate;
      switch (targetType) {
        case INTEGER:
          return bd.intValue();
        case DOUBLE:
          return bd.doubleValue();
        case FLOAT:
          return bd.floatValue();
        default:
          return bd;
      }
    } else {
      return candidate;
    }
  }
}