/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.expression.function.FunctionProperties;

public class WeekDayFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    LocalDate candidateDate =
        LocalDateTime.ofInstant(InstantUtils.convertToInstant(args[0], sqlTypeName, false), ZoneOffset.UTC)
            .toLocalDate();
    if (sqlTypeName == SqlTypeName.TIME) {
      return formatNow(new FunctionProperties().getQueryStartClock()).getDayOfWeek().getValue() - 1;
    } else {
      return candidateDate.getDayOfWeek().getValue() - 1;
    }
  }
}
