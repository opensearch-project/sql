/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Utility methods for to derive types, containing special handling logics for user-defined-types.
 *
 * @see SqlTypeUtil utilities used during SQL validation or type derivation.
 */
@UtilityClass
public class OpenSearchTypeUtil {
  /**
   * Whether a given RelDataType is a user-defined type (UDT)
   *
   * @param type the RelDataType to check
   * @return true if the type is a user-defined type, false otherwise
   */
  public static boolean isUserDefinedType(RelDataType type) {
    return type instanceof AbstractExprRelDataType<?>;
  }

  /**
   * Checks if the RelDataType represents a numeric type. Supports standard SQL numeric types
   * (INTEGER, BIGINT, SMALLINT, TINYINT, FLOAT, DOUBLE, DECIMAL, REAL), OpenSearch UDT numeric
   * types, and string types (VARCHAR, CHAR).
   *
   * @param fieldType the RelDataType to check
   * @return true if the type is numeric or string, false otherwise
   */
  public static boolean isNumericOrCharacter(RelDataType fieldType) {
    // Check for OpenSearch UDT numeric types
    if (isUserDefinedType(fieldType)) {
      AbstractExprRelDataType<?> exprType = (AbstractExprRelDataType<?>) fieldType;
      ExprType udtType = exprType.getExprType();
      return ExprCoreType.numberTypes().contains(udtType);
    }

    // Check standard SQL numeric types & string types (VARCHAR, CHAR)
    if (SqlTypeUtil.isNumeric(fieldType) || SqlTypeUtil.isCharacter(fieldType)) {
      return true;
    }

    return false;
  }

  /**
   * Checks if the RelDataType represents a time-based field (timestamp, date, or time). Supports
   * both standard SQL time types (including TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE, TIME,
   * and their timezone variants) and OpenSearch UDT time types.
   *
   * @param fieldType the RelDataType to check
   * @return true if the type is time-based, false otherwise
   */
  public static boolean isDatetime(RelDataType fieldType) {
    // Check standard SQL time types
    if (SqlTypeUtil.isDatetime(fieldType)) {
      return true;
    }

    // Check for OpenSearch UDT types (EXPR_TIMESTAMP mapped to VARCHAR)
    if (isUserDefinedType(fieldType)) {
      AbstractExprRelDataType<?> exprType = (AbstractExprRelDataType<?>) fieldType;
      ExprType udtType = exprType.getExprType();
      return udtType == ExprCoreType.TIMESTAMP
          || udtType == ExprCoreType.DATE
          || udtType == ExprCoreType.TIME;
    }

    return false;
  }

  /**
   * Checks whether a {@link RelDataType} represents a date type.
   *
   * <p>This method returns true for both Calcite's built-in {@link SqlTypeName#DATE} type and
   * OpenSearch's user-defined date type {@link OpenSearchTypeFactory.ExprUDT#EXPR_DATE}.
   *
   * @param type the type to check
   * @return true if the type is a date type (built-in or user-defined), false otherwise
   */
  public static boolean isDate(RelDataType type) {
    if (isUserDefinedType(type)) {
      if (((AbstractExprRelDataType<?>) type).getUdt() == OpenSearchTypeFactory.ExprUDT.EXPR_DATE) {
        return true;
      }
    }
    return SqlTypeName.DATE.equals(type.getSqlTypeName());
  }

  /**
   * Checks whether a {@link RelDataType} represents a timestamp type.
   *
   * <p>This method returns true for both Calcite's built-in {@link SqlTypeName#TIMESTAMP} type and
   * OpenSearch's user-defined timestamp type {@link OpenSearchTypeFactory.ExprUDT#EXPR_TIMESTAMP}.
   *
   * @param type the type to check
   * @return true if the type is a timestamp type (built-in or user-defined), false otherwise
   */
  public static boolean isTimestamp(RelDataType type) {
    if (isUserDefinedType(type)) {
      if (((AbstractExprRelDataType<?>) type).getUdt()
          == OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP) {
        return true;
      }
    }
    return SqlTypeName.TIMESTAMP.equals(type.getSqlTypeName());
  }

  /**
   * Checks whether a {@link RelDataType} represents a time type.
   *
   * <p>This method returns true for both Calcite's built-in {@link SqlTypeName#TIME} type and
   * OpenSearch's user-defined time type {@link OpenSearchTypeFactory.ExprUDT#EXPR_TIME}.
   *
   * @param type the type to check
   * @return true if the type is a time type (built-in or user-defined), false otherwise
   */
  public static boolean isTime(RelDataType type) {
    if (isUserDefinedType(type)) {
      if (((AbstractExprRelDataType<?>) type).getUdt() == OpenSearchTypeFactory.ExprUDT.EXPR_TIME) {
        return true;
      }
    }
    return SqlTypeName.TIME.equals(type.getSqlTypeName());
  }

  /**
   * This method should be used in place for {@link SqlTypeUtil#isCharacter(RelDataType)} because
   * user-defined types also have VARCHAR as their SqlTypeName.
   */
  public static boolean isCharacter(RelDataType type) {
    return !isUserDefinedType(type) && SqlTypeUtil.isCharacter(type);
  }

  /**
   * Checks whether a {@link RelDataType} represents an IP address type.
   *
   * <p>This method returns true only for OpenSearch's user-defined IP type {@link
   * OpenSearchTypeFactory.ExprUDT#EXPR_IP}.
   *
   * @param type the type to check
   * @return true if the type is an IP address type, false otherwise
   */
  public static boolean isIp(RelDataType type) {
    return isIp(type, false);
  }

  /**
   * Checks whether a {@link RelDataType} represents an IP address type. If {@code acceptOther} is
   * set, {@link SqlTypeName#OTHER} is also accepted as an IP type.
   *
   * <p>{@link SqlTypeName#OTHER} is "borrowed" to represent IP type during validation because
   * <i>SqlTypeName.IP</i> does not exist
   *
   * @param type the type to check
   * @param acceptOther whether to accept OTHER as a valid IP type
   * @return true if the type is an IP address type, false otherwise
   */
  public static boolean isIp(RelDataType type, boolean acceptOther) {
    if (isUserDefinedType(type)) {
      return ((AbstractExprRelDataType<?>) type).getUdt() == OpenSearchTypeFactory.ExprUDT.EXPR_IP;
    }
    if (acceptOther) {
      return type.getSqlTypeName() == SqlTypeName.OTHER;
    }
    return false;
  }

  /**
   * Checks whether a {@link RelDataType} represents a binary type.
   *
   * <p>This method returns true for both Calcite's built-in binary types (BINARY, VARBINARY) and
   * OpenSearch's user-defined binary type {@link OpenSearchTypeFactory.ExprUDT#EXPR_BINARY}.
   *
   * @param type the type to check
   * @return true if the type is a binary type (built-in or user-defined), false otherwise
   */
  public static boolean isBinary(RelDataType type) {
    if (isUserDefinedType(type)) {
      return ((AbstractExprRelDataType<?>) type).getUdt()
          == OpenSearchTypeFactory.ExprUDT.EXPR_BINARY;
    }
    return SqlTypeName.BINARY_TYPES.contains(type.getSqlTypeName());
  }

  /**
   * Checks whether a {@link RelDataType} represents a scalar type.
   *
   * <p>Scalar types include all primitive and atomic types such as numeric types (INTEGER, BIGINT,
   * FLOAT, DOUBLE, DECIMAL), string types (VARCHAR, CHAR), boolean, temporal types (DATE, TIME,
   * TIMESTAMP), and special scalar types (IP, BINARY, UUID).
   *
   * <p>This method returns false for composite types including:
   *
   * <ul>
   *   <li>STRUCT types (structured records with named fields)
   *   <li>MAP types (key-value pairs)
   *   <li>ARRAY and MULTISET types (collections)
   *   <li>ROW types (tuples)
   * </ul>
   *
   * @param type the type to check; may be null
   * @return true if the type is a scalar type, false if it is a composite type or null
   */
  public static boolean isScalar(RelDataType type) {
    if (type == null) {
      return false;
    }
    return !type.isStruct()
        && !SqlTypeUtil.isMap(type)
        && !SqlTypeUtil.isCollection(type)
        && !SqlTypeUtil.isRow(type);
  }
}
