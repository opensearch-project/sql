package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.transferUnixTimeStampFromDoubleInput;
import static org.opensearch.sql.utils.DateTimeFormatters.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class UnixTimeStampFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length == 0) {
      LocalDateTime localDateTime = LocalDateTime.now();
      return localDateTime.toEpochSecond(ZoneOffset.UTC);
    }
    Object input = args[0];
    SqlTypeName inputTypes = (SqlTypeName) args[1];
    if (inputTypes == SqlTypeName.DATE) {
      LocalDate localDate = ((java.sql.Date) input).toLocalDate();
      return localDate.toEpochSecond(LocalTime.MIN, ZoneOffset.UTC);
    } else if (inputTypes == SqlTypeName.TIMESTAMP) {
      LocalDateTime localDateTime = ((java.sql.Timestamp) input).toLocalDateTime();
      return localDateTime.toEpochSecond(ZoneOffset.UTC);
    } else {
      return transferUnixTimeStampFromDoubleInput(((Number) input).doubleValue());
    }
  }
}
