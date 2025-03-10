/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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
    if (null == value) {
      return ExprNullValue.of();
    }
    switch (sqlType) {
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
        value = rs.getString(i);
        break;
      case Types.INTEGER:
        value = rs.getInt(i);
        break;
      case Types.BIGINT:
        value = rs.getLong(i);
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        value = rs.getBigDecimal(i);
        break;
      case Types.DOUBLE:
        value = rs.getDouble(i);
        if (Double.isNaN((Double) value)) {
          value = null;
        }
        break;
      case Types.FLOAT:
        value = rs.getFloat(i);
        if (Float.isNaN((Float) value)) {
          value = null;
        }
        break;
      case Types.DATE:
        value = rs.getString(i);
        return value == null ? ExprNullValue.of() : new ExprDateValue((String) value);
      case Types.TIME:
        value = rs.getString(i);
        return value == null ? ExprNullValue.of() : new ExprTimeValue((String) value);
      case Types.TIMESTAMP:
        value = rs.getString(i);
        return value == null ? ExprNullValue.of() : new ExprTimestampValue((String) value);
      case Types.BOOLEAN:
        value = rs.getBoolean(i);
        break;
      case Types.ARRAY:
        value = rs.getArray(i);
        // For calcite
        if (value instanceof ArrayImpl) {
          value = Arrays.asList((Object[]) ((ArrayImpl) value).getArray());
        }
        break;
      default:
        value = rs.getObject(i);
        LOG.warn(
            "Unchecked sql type: {}, return Object type {}",
            sqlType,
            value.getClass().getTypeName());
    }
    return value == null ? ExprNullValue.of() : ExprValueUtils.fromObjectValue(value);
  }
}
