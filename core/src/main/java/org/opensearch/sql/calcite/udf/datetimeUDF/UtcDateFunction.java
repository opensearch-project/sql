/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.expression.function.FunctionProperties;

import static org.opensearch.sql.expression.DSL.utc_date;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcDate;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcTimeStamp;

public class UtcDateFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    return java.sql.Date.valueOf(exprUtcDate(new FunctionProperties()).dateValue());
  }
}
