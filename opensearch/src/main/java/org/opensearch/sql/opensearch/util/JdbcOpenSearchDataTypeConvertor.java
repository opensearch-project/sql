/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** This class is used to convert the data type from JDBC to OpenSearch data type. */
@UtilityClass
public class JdbcOpenSearchDataTypeConvertor {
  private static final Logger LOG = LogManager.getLogger();

  public static ExprType getExprTypeFromSqlType(int sqlType) {
    switch (sqlType) {
      case Types.INTEGER:
        return ExprCoreType.INTEGER;
      case Types.BIGINT:
        return ExprCoreType.LONG;
      case Types.DOUBLE:
      case Types.DECIMAL:
      case Types.NUMERIC:
        return ExprCoreType.DOUBLE;
      case Types.FLOAT:
        return ExprCoreType.FLOAT;
      case Types.DATE:
        return ExprCoreType.DATE;
      case Types.TIMESTAMP:
        return ExprCoreType.TIMESTAMP;
      case Types.BOOLEAN:
        return ExprCoreType.BOOLEAN;
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
        return ExprCoreType.STRING;
      default:
        // TODO unchecked OpenSearchDataType
        return ExprCoreType.UNKNOWN;
    }
  }

  public static ExprValue getExprValueFromSqlType(
      ResultSet rs, int i, int sqlType, RelDataType fieldType) throws SQLException {
    Object value = rs.getObject(i);
    if (value == null) {
      return ExprNullValue.of();
    }

    try {
      switch (sqlType) {
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.LONGVARCHAR:
          return ExprValueUtils.fromObjectValue(rs.getString(i));

        case Types.INTEGER:
          return ExprValueUtils.fromObjectValue(rs.getInt(i));

        case Types.BIGINT:
          return ExprValueUtils.fromObjectValue(rs.getLong(i));

        case Types.DECIMAL:
        case Types.NUMERIC:
          return ExprValueUtils.fromObjectValue(rs.getBigDecimal(i));

        case Types.DOUBLE:
          return ExprValueUtils.fromObjectValue(rs.getDouble(i));

        case Types.FLOAT:
          return ExprValueUtils.fromObjectValue(rs.getFloat(i));

        case Types.DATE:
          String dateStr = rs.getString(i);
          return new ExprDateValue(dateStr);

        case Types.TIME:
          Time timeStr = rs.getTime(i);
          long millisecond = (timeStr.getTime() + 3600000L) % 1000L;
          LocalTime localTime = timeStr.toLocalTime().plusNanos(millisecond * 1000000);
          return new ExprTimeValue(localTime);

        case Types.TIMESTAMP:
          String timestampStr = rs.getString(i);
          return new ExprTimestampValue(timestampStr);

        case Types.BOOLEAN:
          return ExprValueUtils.fromObjectValue(rs.getBoolean(i));

        case Types.ARRAY:
          Array array = rs.getArray(i);
          if (array instanceof ArrayImpl) {
            return ExprValueUtils.fromObjectValue(
                Arrays.asList((Object[]) ((ArrayImpl) value).getArray()));
          }
          return ExprValueUtils.fromObjectValue(array);

        default:
          LOG.warn(
              "Unchecked sql type: {}, return Object type {}",
              sqlType,
              value.getClass().getTypeName());
          return ExprValueUtils.fromObjectValue(value);
      }
    } catch (SQLException e) {
      LOG.error("Error converting SQL type {}: {}", sqlType, e.getMessage());
      throw e;
    }
  }
}
