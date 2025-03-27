/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;
import static org.opensearch.sql.utils.DateTimeFormatters.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class UnixTimeStampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    if (args.length == 0) {
      return unixTimeStamp(new FunctionProperties().getQueryStartClock()).longValue();
    }
    Object input = args[0];
    if (Objects.isNull(input)) {
      return null;
    }
    SqlTypeName inputTypes = (SqlTypeName) args[1];
    ExprValue inputValue;
    if (inputTypes == SqlTypeName.DATE) {
      inputValue =
          new ExprDateValue(
              LocalDateTime.ofInstant(InstantUtils.convertToInstant(input, inputTypes, false), ZoneOffset.UTC)
                  .toLocalDate());
    } else if (inputTypes == SqlTypeName.TIMESTAMP) {
      inputValue =
          new ExprTimestampValue(
              LocalDateTime.ofInstant(InstantUtils.convertToInstant(input, inputTypes, false), ZoneOffset.UTC));
    } else {
      inputValue = new ExprLongValue((long) input);
    }
    return (double) unixTimeStampOf(inputValue).longValue();
  }
}
