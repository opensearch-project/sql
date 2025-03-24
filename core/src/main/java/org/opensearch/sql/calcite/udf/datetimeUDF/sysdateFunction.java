package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;

import java.time.Clock;
import java.time.LocalDateTime;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class sysdateFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    LocalDateTime localDateTime;
    if (args.length == 0) {
      localDateTime = formatNow(Clock.systemDefaultZone(), 0);
    } else {
      localDateTime = formatNow(Clock.systemDefaultZone(), (int) args[0]);
    }
    return java.sql.Timestamp.valueOf(localDateTime);
  }
}
