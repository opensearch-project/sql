package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTime;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprSecToTimeWithNanos;

public class secondToTimeFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        Number candidate = (Number) args[0];
        ExprValue returnTimeValue;
        ExprValue transferredValue;
        if (candidate instanceof Long) {
            transferredValue = ExprValueUtils.longValue((Long) candidate);
            returnTimeValue = exprSecToTime(transferredValue);
        } else if (candidate instanceof Integer) {
            transferredValue = ExprValueUtils.integerValue((Integer) candidate);
            returnTimeValue = exprSecToTime(transferredValue);
        } else if (candidate instanceof Double) {
            transferredValue = ExprValueUtils.doubleValue((Double) candidate);
            returnTimeValue = exprSecToTimeWithNanos(transferredValue);
        } else {
            transferredValue = ExprValueUtils.floatValue((Float) candidate);
            returnTimeValue = exprSecToTimeWithNanos(transferredValue);
        }
        return returnTimeValue.timeValue().toSecondOfDay() * 1000;
        //java.sql.Time demo = new java.sql.Time(returnTimeValue.timeValue().toNanoOfDay() / 1000000L);
        //return new java.sql.Time(returnTimeValue.timeValue().toNanoOfDay() / 1000000L);
    }

}
