/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

// TODO: Fix MICROSECOND precision, it is not correct with Calcite timestamp
public class ExtractFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    Object argPart = args[0];
    Object argTimestamp = args[1];
    SqlTypeName argType = (SqlTypeName) args[2];

    Instant datetimeInstant = InstantUtils.convertToInstant(argTimestamp, argType, false);

    return DateTimeFunctions.formatExtractFunction(
            new ExprStringValue(argPart.toString()), new ExprTimestampValue(datetimeInstant))
        .longValue();
  }
}
