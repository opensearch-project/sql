/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;

public class PostprocessForUDTFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object candidate = args[0];
    if (Objects.isNull(candidate)) {
      return null;
    }
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    return internalDatetimeToExprValue(candidate, sqlTypeName);
  }

  public static Object internalDatetimeToExprValue(Object internalValue, SqlTypeName sqlTypeName) {
    Instant instant = InstantUtils.convertToInstant(internalValue, sqlTypeName);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    return switch (sqlTypeName) {
      case DATE -> new ExprDateValue(localDateTime.toLocalDate()).valueForCalcite();
      case TIME -> new ExprTimeValue(localDateTime.toLocalTime()).valueForCalcite();
      case TIMESTAMP -> new ExprTimestampValue(localDateTime).valueForCalcite();
      default -> throw new IllegalArgumentException("Unsupported datetime type: " + sqlTypeName);
    };
  }
}
