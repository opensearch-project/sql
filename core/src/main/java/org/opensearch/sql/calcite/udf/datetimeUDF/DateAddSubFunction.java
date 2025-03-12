package org.opensearch.sql.calcite.udf.datetimeUDF;

import java.sql.Timestamp;
import java.time.*;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.DateTimeApplyUtils;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

public class DateAddSubFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    if (args.length < 5) {
      throw new IllegalArgumentException("Mismatch arguments: expected 5 but got " + args.length);
    }
    Object argUnit = args[0];
    Object argNumInterval = args[1];
    Object argBase = args[2];
    Object argBaseType = args[3];
    Object argIsAdd = args[4];

    assert argUnit instanceof TimeUnit;
    assert argNumInterval instanceof Number;
    assert argBaseType instanceof SqlTypeName;
    assert argIsAdd instanceof Boolean;
    TimeUnit unit = (TimeUnit) argUnit;
    long interval = ((Number) argNumInterval).longValue();
    SqlTypeName sqlTypeName = (SqlTypeName) argBaseType;
    boolean isAdd = (Boolean) argIsAdd;
    Instant base;
    switch (sqlTypeName) {
      case DATE:
        // Convert it to milliseconds
        base = InstantUtils.fromInternalDate(((Number) argBase).intValue());
        break;
      case TIME:
        // Add an offset of today's date at 00:00:00
        base = InstantUtils.fromInternalTime(((Number) argBase).intValue());
        break;
      case TIMESTAMP:
        base = InstantUtils.fromEpochMills(((Number) argBase).longValue());
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid argument type. Must be DATE, TIME, or TIMESTAMP, but got " + sqlTypeName);
    }

    Instant newInstant =
        DateTimeApplyUtils.applyInterval(
            base, Duration.ofMillis(unit.multiplier.longValue() * interval), isAdd);
    return Timestamp.valueOf(LocalDateTime.ofInstant(newInstant, ZoneOffset.UTC));
  }
}
