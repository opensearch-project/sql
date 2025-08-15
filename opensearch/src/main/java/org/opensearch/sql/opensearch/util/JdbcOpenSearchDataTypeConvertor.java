/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.Arrays;
import java.util.LinkedHashMap;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.calcite.type.ExprJavaType;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;

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

  public static ExprValue getExprValueFromRelDataType(Object value, RelDataType fieldType)
      throws SQLException {
    if (value == null) {
      return ExprNullValue.of();
    }

    if (fieldType instanceof ExprJavaType && value instanceof ExprValue) {
      return (ExprValue) value;
    }

    try {
      switch (fieldType.getSqlTypeName()) {
        case GEOMETRY:
          Point geoPoint = (Point) value;
          return new OpenSearchExprGeoPointValue(geoPoint.getY(), geoPoint.getX());
        case DATE:
          return new ExprDateValue(value.toString());
        case TIME:
          return new ExprTimeValue(value.toString());
        case TIMESTAMP:
          return new ExprTimestampValue(value.toString());
        case ARRAY:
          if (value instanceof ArrayImpl) {
            return ExprValueUtils.fromObjectValue(
                Arrays.asList((Object[]) ((ArrayImpl) value).getArray()));
          }
          return ExprValueUtils.fromObjectValue(value);
        case ROW:
          Object[] attributes = ((Struct) value).getAttributes();
          java.util.Map<String, ExprValue> tupleMap = new LinkedHashMap<>();
          for (int i = 0; i < attributes.length; i++) {
            RelDataTypeField field = fieldType.getFieldList().get(i);
            tupleMap.put(
                field.getName(), getExprValueFromRelDataType(attributes[i], field.getType()));
          }
          return ExprTupleValue.fromExprValueMap(tupleMap);
        default:
          return ExprValueUtils.fromObjectValue(value);
      }
    } catch (SQLException e) {
      LOG.error(
          "Error converting RelDataType {} with field value {}: {}",
          fieldType,
          value,
          e.getMessage());
      throw e;
    }
  }
}
