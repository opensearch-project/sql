/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.math.BigDecimal;
import org.apache.calcite.sql.type.SqlTypeName;

public class LambdaUtils {
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
