package org.opensearch.sql.calcite.udf.datetimeUDF;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;
import org.opensearch.sql.calcite.utils.datetime.InstantUtils;

public class PostprocessForUDTFunction implements UserDefinedFunction {
  @Override
  public Object eval(Object... args) {
    Object candidate = args[0];
    if (Objects.isNull(candidate)) {
      return null;
    }
    SqlTypeName sqlTypeName = (SqlTypeName) args[1];
    Instant instant = InstantUtils.convertToInstant(candidate, sqlTypeName, false);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    switch (sqlTypeName) {
      case DATE:
        return formatDate(localDateTime.toLocalDate());
      case TIME:
        return formatTime(localDateTime.toLocalTime());
      case TIMESTAMP:
        return formatTimestamp(localDateTime);
      default:
        throw new IllegalArgumentException("Unsupported datetime type: " + sqlTypeName);
    }
  }
}
