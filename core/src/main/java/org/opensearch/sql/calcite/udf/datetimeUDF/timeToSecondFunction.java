/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimeValue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprTimeToSec;

public class timeToSecondFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        SqlTypeName timeType = (SqlTypeName) args[1];
        Instant time = InstantUtils.convertToInstant(args[0], timeType);
        LocalTime candidateTime = LocalDateTime.ofInstant(time, ZoneOffset.UTC).toLocalTime();
        return exprTimeToSec(new ExprTimeValue(candidateTime)).longValue();
    }
}
