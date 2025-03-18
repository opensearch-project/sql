/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.expression.function.FunctionProperties;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprUtcTimeStamp;

public class UtcTimeStampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    return java.sql.Timestamp.valueOf(LocalDateTime.ofInstant(exprUtcTimeStamp(new FunctionProperties()).timestampValue(), ZoneOffset.UTC));
  }
}
