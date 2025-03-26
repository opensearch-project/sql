package org.opensearch.sql.calcite.udf.datetimeUDF;

import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Arrays;
import org.apache.calcite.sql.type.SqlTypeName;
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
    UserDefinedFunctionUtils.validateArgumentCount("MINUTE_OF_DAY", 2, args.length, false);
    UserDefinedFunctionUtils.validateArgumentTypes(
        Arrays.asList(args), ImmutableList.of(Number.class, SqlTypeName.class));
    Instant timestamp = InstantUtils.convertToInstant(args[0], (SqlTypeName) args[1], false);
    return DateTimeFunctions.exprMinuteOfDay(new ExprTimestampValue(timestamp)).integerValue();
  }
}
