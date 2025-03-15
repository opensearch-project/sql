package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;

import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferCalciteValueToExprTimeStampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSeconds;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprToSecondsForIntType;

/**
 * Use function: DateFunctions::to_seconds
 *
 */

public class toSecondsFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("To seconds Expected at least one arguments, got " + (args.length - 1));
        }
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        switch (sqlTypeName) {
            case DATE, TIME, TIMESTAMP:
                ExprTimestampValue dateTimeValue = transferCalciteValueToExprTimeStampValue(sqlTypeName, args[1]);
                return exprToSeconds(dateTimeValue).longValue();
            default:
                return exprToSecondsForIntType(new ExprLongValue((Number) args[1]));
        }
    }
}
