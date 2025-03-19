/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

public class WeekDayFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        LocalDate candidateDate = LocalDateTime.ofInstant(InstantUtils.convertToInstant(args[0], sqlTypeName), ZoneOffset.UTC).toLocalDate();
        if (sqlTypeName == SqlTypeName.TIME) {
            return  formatNow(new FunctionProperties().getQueryStartClock()).getDayOfWeek().getValue()
                    - 1;
        }
        else {
            return candidateDate.getDayOfWeek().getValue() - 1;
        }
    }
}
