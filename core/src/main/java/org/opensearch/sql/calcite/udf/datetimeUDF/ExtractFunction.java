/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class ExtractFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object argPart = args[0];
    Object argTimestamp = args[1];

    Instant datetimeInstant = InstantUtils.fromEpochMills(((Number) argTimestamp).longValue());
    LocalDateTime datetime = LocalDateTime.ofInstant(datetimeInstant, ZoneOffset.UTC);

    return DateTimeFunctions.formatExtractFunction(
            new ExprStringValue(argPart.toString()), new ExprTimestampValue(datetime))
        .longValue();
  }
}
