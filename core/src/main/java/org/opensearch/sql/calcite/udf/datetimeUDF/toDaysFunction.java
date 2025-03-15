package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;

import static org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils.transferCalciteValueToExprTimeStampValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;

public class toDaysFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("To seconds Expected at least one arguments, got " + (args.length - 1));
        }
        SqlTypeName sqlTypeName = (SqlTypeName) args[1];
        ExprValue candidateValue;
        switch (sqlTypeName) {
            case DATE, TIME, TIMESTAMP:
                candidateValue = transferCalciteValueToExprTimeStampValue(sqlTypeName, args[1]);
                break;
            default:
                candidateValue = new ExprLongValue((Number) args[1]);
        }
        return exprToDays(candidateValue);
    }
}
