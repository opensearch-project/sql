package org.opensearch.sql.calcite.udf.datetimeUDF;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.datetime.DateTimeFunctions;

/** minute(time) returns the amount of minutes in the day, in the range of 0 to 1439. */
public class MinuteOfDay implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (UserDefinedFunctionUtils.containsNull(args)) {
      return null;
    }
    UserDefinedFunctionUtils.validateArgumentCount("MINUTE_OF_DAY", 1, args.length, false);
    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args), ImmutableList.of(Number.class));
    return DateTimeFunctions.exprMinuteOfDay(
            new ExprTimestampValue(InstantUtils.fromEpochMills(((Number) args[0]).longValue())))
        .integerValue();
  }
}
