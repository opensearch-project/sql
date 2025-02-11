/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

@UtilityClass
public class JdbcUtil {

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
      default:
        return ExprCoreType.STRING;
    }
  }

  public static ExprValue getExprValueFromSqlType(ResultSet rs, int i, int sqlType)
      throws SQLException {
    Object value;
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
        break;
      case Types.FLOAT:
        value = rs.getFloat(i);
        break;
      case Types.DATE:
        value = rs.getDate(i);
        break;
      case Types.TIMESTAMP:
        value = rs.getTimestamp(i);
        break;
      case Types.BOOLEAN:
        value = rs.getBoolean(i);
        break;
      default:
        value = rs.getObject(i);
    }
    return ExprValueUtils.fromObjectValue(value);
  }
}
