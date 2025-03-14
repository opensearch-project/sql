package org.opensearch.sql.calcite.udf.datetimeUDF;

import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

public class GetFormatFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object argType = args[0];
    Object argStandard = args[1];

    ExprValue fmt =
        DateTimeFunctions.exprGetFormat(
            new ExprStringValue(argType.toString()), new ExprStringValue(argStandard.toString()));

    return fmt.stringValue();
  }
}
