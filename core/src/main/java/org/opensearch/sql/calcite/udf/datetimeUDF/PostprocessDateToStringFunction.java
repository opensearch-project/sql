/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.formatTimestampWithoutUnnecessaryNanos;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.restoreFunctionProperties;
import static org.opensearch.sql.calcite.utils.datetime.InstantUtils.parseStringToTimestamp;

import java.time.LocalDateTime;
import java.util.Objects;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.expression.function.FunctionProperties;

public class PostprocessDateToStringFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object candidate = args[0];
    if (Objects.isNull(candidate)) {
      return null;
    }
    FunctionProperties restored = restoreFunctionProperties(args[args.length - 1]);
    LocalDateTime localDateTime = parseStringToTimestamp((String) candidate, restored);
    String formatted = formatTimestampWithoutUnnecessaryNanos(localDateTime);
    return formatted;
  }
}
