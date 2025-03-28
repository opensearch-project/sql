/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class YearWeekFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {

    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }

    Instant basetime;
    int mode;
    if (Objects.isNull(args[0])) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    LocalDate candidateDate;
    SqlTypeName sqlTypeName;
    if (args.length == 3) {
      sqlTypeName = (SqlTypeName) args[1];
      basetime = InstantUtils.convertToInstant(args[0], sqlTypeName, false);
      mode = 0;
    } else {
      sqlTypeName = (SqlTypeName) args[2];
      basetime = InstantUtils.convertToInstant(args[0], sqlTypeName, false);
      mode = (int) args[1];
    }
    if (sqlTypeName == SqlTypeName.TIME) {
      candidateDate = LocalDateTime.now(restored.getQueryStartClock()).toLocalDate();
    } else {
      candidateDate = LocalDateTime.ofInstant(basetime, ZoneOffset.UTC).toLocalDate();
    }
    ExprDateValue dateValue = new ExprDateValue(candidateDate);
    ExprValue yearWeekValue = exprYearweek(dateValue, new ExprIntegerValue(mode));
    return yearWeekValue.integerValue();
  }
}
