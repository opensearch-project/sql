package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprStrToDate;

public class strToDateFunction implements UserDefinedFunction {
    @Override
    public Object eval(Object... args) {
        String candidate =(String) args[0];
        String format=(String) args[1];
        ExprValue returnValue = exprStrToDate(FunctionProperties.None, new ExprStringValue(candidate), new ExprStringValue(format));
        return java.sql.Timestamp.valueOf(LocalDateTime.ofInstant(returnValue.timestampValue(), ZoneOffset.UTC));
    }
}
