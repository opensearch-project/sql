/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class OpenSearchTypeUtilTest {

  // ==================== isUserDefinedType tests ====================

  @Test
  public void testIsUserDefinedType_withUDT_returnsTrue() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    assertTrue(OpenSearchTypeUtil.isUserDefinedType(dateUdt));
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(timeUdt));
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(timestampUdt));
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(ipUdt));
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(binaryUdt));
  }

  @Test
  public void testIsUserDefinedType_withStandardType_returnsFalse() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);

    assertFalse(OpenSearchTypeUtil.isUserDefinedType(intType));
    assertFalse(OpenSearchTypeUtil.isUserDefinedType(varcharType));
    assertFalse(OpenSearchTypeUtil.isUserDefinedType(dateType));
  }

  // ==================== isNumericOrCharacter tests ====================

  @Test
  public void testIsNumericOrCharacter_withNumericTypes_returnsTrue() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    RelDataType smallintType = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);
    RelDataType tinyintType = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);
    RelDataType doubleType = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
    RelDataType floatType = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);
    RelDataType realType = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
    RelDataType decimalType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL);

    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(intType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(bigintType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(smallintType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(tinyintType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(doubleType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(floatType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(realType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(decimalType));
  }

  @Test
  public void testIsNumericOrCharacter_withCharacterTypes_returnsTrue() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR);

    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(varcharType));
    assertTrue(OpenSearchTypeUtil.isNumericOrCharacter(charType));
  }

  @Test
  public void testIsNumericOrCharacter_withNonNumericTypes_returnsFalse() {
    RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType binaryType = TYPE_FACTORY.createSqlType(SqlTypeName.BINARY);

    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(booleanType));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(dateType));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(timestampType));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(binaryType));
  }

  @Test
  public void testIsNumericOrCharacter_withVarcharBasedUDTs_returnsFalse() {
    // These UDTs wrap VARCHAR via ExprSqlType, so SqlTypeUtil.isCharacter returns true
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(binaryUdt));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(dateUdt));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(timeUdt));
    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(timestampUdt));
  }

  @Test
  public void testIsNumericOrCharacter_withJavaTypeBasedUDT_returnsFalse() {
    // IP UDT wraps JavaType (not VARCHAR), so it doesn't pass the isCharacter check
    // and IP is not a numeric type in ExprCoreType.numberTypes()
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    assertFalse(OpenSearchTypeUtil.isNumericOrCharacter(ipUdt));
  }

  // ==================== isDatetime tests ====================

  @Test
  public void testIsDatetime_withStandardDatetimeTypes_returnsTrue() {
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timeType = TYPE_FACTORY.createSqlType(SqlTypeName.TIME);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);

    assertTrue(OpenSearchTypeUtil.isDatetime(dateType));
    assertTrue(OpenSearchTypeUtil.isDatetime(timeType));
    assertTrue(OpenSearchTypeUtil.isDatetime(timestampType));
  }

  @Test
  public void testIsDatetime_withUDTDatetimeTypes_returnsTrue() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    assertTrue(OpenSearchTypeUtil.isDatetime(dateUdt));
    assertTrue(OpenSearchTypeUtil.isDatetime(timeUdt));
    assertTrue(OpenSearchTypeUtil.isDatetime(timestampUdt));
  }

  @Test
  public void testIsDatetime_withNonDatetimeTypes_returnsFalse() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    assertFalse(OpenSearchTypeUtil.isDatetime(intType));
    assertFalse(OpenSearchTypeUtil.isDatetime(varcharType));
    assertFalse(OpenSearchTypeUtil.isDatetime(ipUdt));
    assertFalse(OpenSearchTypeUtil.isDatetime(binaryUdt));
  }

  // ==================== isDate tests ====================

  @Test
  public void testIsDate_withStandardDateType_returnsTrue() {
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    assertTrue(OpenSearchTypeUtil.isDate(dateType));
  }

  @Test
  public void testIsDate_withUDTDateType_returnsTrue() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    assertTrue(OpenSearchTypeUtil.isDate(dateUdt));
  }

  @Test
  public void testIsDate_withNonDateTypes_returnsFalse() {
    RelDataType timeType = TYPE_FACTORY.createSqlType(SqlTypeName.TIME);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    assertFalse(OpenSearchTypeUtil.isDate(timeType));
    assertFalse(OpenSearchTypeUtil.isDate(timestampType));
    assertFalse(OpenSearchTypeUtil.isDate(timeUdt));
    assertFalse(OpenSearchTypeUtil.isDate(timestampUdt));
  }

  // ==================== isTimestamp tests ====================

  @Test
  public void testIsTimestamp_withStandardTimestampType_returnsTrue() {
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    assertTrue(OpenSearchTypeUtil.isTimestamp(timestampType));
  }

  @Test
  public void testIsTimestamp_withUDTTimestampType_returnsTrue() {
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    assertTrue(OpenSearchTypeUtil.isTimestamp(timestampUdt));
  }

  @Test
  public void testIsTimestamp_withNonTimestampTypes_returnsFalse() {
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timeType = TYPE_FACTORY.createSqlType(SqlTypeName.TIME);
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);

    assertFalse(OpenSearchTypeUtil.isTimestamp(dateType));
    assertFalse(OpenSearchTypeUtil.isTimestamp(timeType));
    assertFalse(OpenSearchTypeUtil.isTimestamp(dateUdt));
    assertFalse(OpenSearchTypeUtil.isTimestamp(timeUdt));
  }

  // ==================== isTime tests ====================

  @Test
  public void testIsTime_withStandardTimeType_returnsTrue() {
    RelDataType timeType = TYPE_FACTORY.createSqlType(SqlTypeName.TIME);
    assertTrue(OpenSearchTypeUtil.isTime(timeType));
  }

  @Test
  public void testIsTime_withUDTTimeType_returnsTrue() {
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    assertTrue(OpenSearchTypeUtil.isTime(timeUdt));
  }

  @Test
  public void testIsTime_withNonTimeTypes_returnsFalse() {
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    assertFalse(OpenSearchTypeUtil.isTime(dateType));
    assertFalse(OpenSearchTypeUtil.isTime(timestampType));
    assertFalse(OpenSearchTypeUtil.isTime(dateUdt));
    assertFalse(OpenSearchTypeUtil.isTime(timestampUdt));
  }

  // ==================== isCharacter tests ====================

  @Test
  public void testIsCharacter_withCharacterTypes_returnsTrue() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR);

    assertTrue(OpenSearchTypeUtil.isCharacter(varcharType));
    assertTrue(OpenSearchTypeUtil.isCharacter(charType));
  }

  @Test
  public void testIsCharacter_withUDTTypes_returnsFalse() {
    // UDTs have VARCHAR as their SqlTypeName but should not be considered character types
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    assertFalse(OpenSearchTypeUtil.isCharacter(dateUdt));
    assertFalse(OpenSearchTypeUtil.isCharacter(timeUdt));
    assertFalse(OpenSearchTypeUtil.isCharacter(timestampUdt));
    assertFalse(OpenSearchTypeUtil.isCharacter(ipUdt));
    assertFalse(OpenSearchTypeUtil.isCharacter(binaryUdt));
  }

  @Test
  public void testIsCharacter_withNonCharacterTypes_returnsFalse() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);

    assertFalse(OpenSearchTypeUtil.isCharacter(intType));
    assertFalse(OpenSearchTypeUtil.isCharacter(booleanType));
    assertFalse(OpenSearchTypeUtil.isCharacter(dateType));
  }

  // ==================== isIp tests (no acceptOther parameter) ====================

  @Test
  public void testIsIp_withUDTIpType_returnsTrue() {
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    assertTrue(OpenSearchTypeUtil.isIp(ipUdt));
  }

  @Test
  public void testIsIp_withNonIpTypes_returnsFalse() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType otherType = TYPE_FACTORY.createSqlType(SqlTypeName.OTHER);
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);

    assertFalse(OpenSearchTypeUtil.isIp(varcharType));
    assertFalse(OpenSearchTypeUtil.isIp(otherType)); // Without acceptOther, OTHER is not IP
    assertFalse(OpenSearchTypeUtil.isIp(dateUdt));
  }

  // ==================== isIp tests (with acceptOther parameter) ====================

  @Test
  public void testIsIp_withAcceptOther_acceptsOtherType() {
    RelDataType otherType = TYPE_FACTORY.createSqlType(SqlTypeName.OTHER);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    assertTrue(OpenSearchTypeUtil.isIp(otherType, true));
    assertTrue(OpenSearchTypeUtil.isIp(ipUdt, true));
    assertFalse(OpenSearchTypeUtil.isIp(otherType, false));
  }

  @Test
  public void testIsIp_withAcceptOther_rejectsNonIpTypes() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    assertFalse(OpenSearchTypeUtil.isIp(varcharType, true));
    assertFalse(OpenSearchTypeUtil.isIp(intType, true));
  }

  // ==================== isBinary tests ====================

  @Test
  public void testIsBinary_withStandardBinaryTypes_returnsTrue() {
    RelDataType binaryType = TYPE_FACTORY.createSqlType(SqlTypeName.BINARY);
    RelDataType varbinaryType = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    assertTrue(OpenSearchTypeUtil.isBinary(binaryType));
    assertTrue(OpenSearchTypeUtil.isBinary(varbinaryType));
  }

  @Test
  public void testIsBinary_withUDTBinaryType_returnsTrue() {
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    assertTrue(OpenSearchTypeUtil.isBinary(binaryUdt));
  }

  @Test
  public void testIsBinary_withNonBinaryTypes_returnsFalse() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    assertFalse(OpenSearchTypeUtil.isBinary(varcharType));
    assertFalse(OpenSearchTypeUtil.isBinary(intType));
    assertFalse(OpenSearchTypeUtil.isBinary(ipUdt));
  }

  // ==================== isScalar tests ====================

  @Test
  public void testIsScalar_withNull_returnsFalse() {
    assertFalse(OpenSearchTypeUtil.isScalar(null));
  }

  @Test
  public void testIsScalar_withScalarTypes_returnsTrue() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    assertTrue(OpenSearchTypeUtil.isScalar(intType));
    assertTrue(OpenSearchTypeUtil.isScalar(varcharType));
    assertTrue(OpenSearchTypeUtil.isScalar(booleanType));
    assertTrue(OpenSearchTypeUtil.isScalar(dateType));
    assertTrue(OpenSearchTypeUtil.isScalar(timestampType));
    assertTrue(OpenSearchTypeUtil.isScalar(ipUdt));
    assertTrue(OpenSearchTypeUtil.isScalar(binaryUdt));
  }

  @Test
  public void testIsScalar_withStructType_returnsFalse() {
    RelDataType structType =
        TYPE_FACTORY.createStructType(
            java.util.List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
            java.util.List.of("id", "name"));

    assertFalse(OpenSearchTypeUtil.isScalar(structType));
  }

  @Test
  public void testIsScalar_withMapType_returnsFalse() {
    RelDataType mapType =
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

    assertFalse(OpenSearchTypeUtil.isScalar(mapType));
  }

  @Test
  public void testIsScalar_withArrayType_returnsFalse() {
    RelDataType arrayType =
        TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), -1);

    assertFalse(OpenSearchTypeUtil.isScalar(arrayType));
  }

  @Test
  public void testIsScalar_withMultisetType_returnsFalse() {
    RelDataType multisetType =
        TYPE_FACTORY.createMultisetType(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), -1);

    assertFalse(OpenSearchTypeUtil.isScalar(multisetType));
  }
}
