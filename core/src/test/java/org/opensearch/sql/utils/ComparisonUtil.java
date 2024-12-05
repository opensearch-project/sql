/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.opensearch.sql.data.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getFloatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getLongValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getStringValue;
import static org.opensearch.sql.utils.DateTimeUtils.extractTimestamp;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.FunctionProperties;

public class ComparisonUtil {

  /**
   * Util to compare the object (integer, long, float, double, string) values. Allows comparing
   * different datetime types and requires `FunctionProperties` object for that.
   */
  public static int compare(FunctionProperties functionProperties, ExprValue v1, ExprValue v2) {
    if (v1.isMissing() || v2.isMissing()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on missing value");
    } else if (v1.isNull() || v2.isNull()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on null value");
    } else if (v1.type() != v2.type() && v1.isDateTime() && v2.isDateTime()) {
      return extractTimestamp(v1, functionProperties)
          .compareTo(extractTimestamp(v2, functionProperties));
    }
    return compare(v1, v2);
  }

  /** Util to compare the object (integer, long, float, double, string) values. */
  public static int compare(ExprValue v1, ExprValue v2) {
    if (v1.isMissing() || v2.isMissing()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on missing value");
    } else if (v1.isNull() || v2.isNull()) {
      throw new ExpressionEvaluationException("invalid to call compare operation on null value");
    } else if (v1.type() != v2.type()) {
      throw new ExpressionEvaluationException(
          "invalid to call compare operation on values of different types");
    }

    return switch ((ExprCoreType) v1.type()) {
      case BYTE -> v1.byteValue().compareTo(v2.byteValue());
      case SHORT -> v1.shortValue().compareTo(v2.shortValue());
      case INTEGER -> getIntegerValue(v1).compareTo(getIntegerValue(v2));
      case LONG -> getLongValue(v1).compareTo(getLongValue(v2));
      case FLOAT -> getFloatValue(v1).compareTo(getFloatValue(v2));
      case DOUBLE -> getDoubleValue(v1).compareTo(getDoubleValue(v2));
      case STRING -> getStringValue(v1).compareTo(getStringValue(v2));
      case BOOLEAN -> v1.booleanValue().compareTo(v2.booleanValue());
      case TIME -> v1.timeValue().compareTo(v2.timeValue());
      case DATE -> v1.dateValue().compareTo(v2.dateValue());
      case TIMESTAMP -> v1.timestampValue().compareTo(v2.timestampValue());
      default -> throw new ExpressionEvaluationException(
          String.format("%s instances are not comparable", v1.getClass().getSimpleName()));
    };
  }
}
