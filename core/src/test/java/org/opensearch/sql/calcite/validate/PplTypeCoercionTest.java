/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;

public class PplTypeCoercionTest {

  private PplTypeCoercion typeCoercion;

  @BeforeEach
  public void setUp() {
    SqlValidator mockValidator = Mockito.mock(SqlValidator.class);
    typeCoercion = new PplTypeCoercion(TYPE_FACTORY, mockValidator);
  }

  // ==================== implicitCast tests ====================

  @Test
  public void testImplicitCast_stringToDatetime_returnsTimestampUDT() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = typeCoercion.implicitCast(varcharType, SqlTypeFamily.DATETIME);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTimestamp(result));
  }

  @Test
  public void testImplicitCast_dateTypeFamily_returnsDateUDT() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = typeCoercion.implicitCast(varcharType, SqlTypeFamily.DATE);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isDate(result));
  }

  @Test
  public void testImplicitCast_timeTypeFamily_returnsTimeUDT() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = typeCoercion.implicitCast(varcharType, SqlTypeFamily.TIME);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTime(result));
  }

  @Test
  public void testImplicitCast_timestampTypeFamily_returnsTimestampUDT() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = typeCoercion.implicitCast(varcharType, SqlTypeFamily.TIMESTAMP);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTimestamp(result));
  }

  @Test
  public void testImplicitCast_numericTypes_returnsStandardType() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = typeCoercion.implicitCast(intType, SqlTypeFamily.NUMERIC);

    assertNotNull(result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }

  @Test
  public void testImplicitCast_incompatibleTypes_returnsNull() {
    RelDataType booleanType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

    // Boolean cannot be implicitly cast to NUMERIC
    RelDataType result = typeCoercion.implicitCast(booleanType, SqlTypeFamily.NUMERIC);

    assertNull(result);
  }

  @Test
  public void testImplicitCast_preservesNullability() {
    RelDataType nullableVarchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);
    RelDataType nonNullableVarchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, false);

    RelDataType nullableResult = typeCoercion.implicitCast(nullableVarchar, SqlTypeFamily.DATE);
    RelDataType nonNullableResult =
        typeCoercion.implicitCast(nonNullableVarchar, SqlTypeFamily.DATE);

    assertNotNull(nullableResult);
    assertNotNull(nonNullableResult);
    assertTrue(nullableResult.isNullable());
  }

  // ==================== commonTypeForBinaryComparison tests ====================

  @Test
  public void testCommonTypeForBinaryComparison_dateAndTime_returnsTimestamp() {
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(dateType, timeUdt);

    assertNotNull(result);
    assertEquals(SqlTypeName.TIMESTAMP, result.getSqlTypeName());
  }

  @Test
  public void testCommonTypeForBinaryComparison_timeAndDate_returnsTimestamp() {
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType dateType = TYPE_FACTORY.createSqlType(SqlTypeName.DATE);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(timeUdt, dateType);

    assertNotNull(result);
    assertEquals(SqlTypeName.TIMESTAMP, result.getSqlTypeName());
  }

  @Test
  public void testCommonTypeForBinaryComparison_timeAndTimestamp_returnsTimestamp() {
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(timeUdt, timestampType);

    assertNotNull(result);
    assertEquals(SqlTypeName.TIMESTAMP, result.getSqlTypeName());
  }

  @Test
  public void testCommonTypeForBinaryComparison_timestampAndTime_returnsTimestamp() {
    RelDataType timestampType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(timestampType, timeUdt);

    assertNotNull(result);
    assertEquals(SqlTypeName.TIMESTAMP, result.getSqlTypeName());
  }

  @Test
  public void testCommonTypeForBinaryComparison_ipAndString_returnsIp() {
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(ipUdt, varcharType);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isIp(result));
  }

  @Test
  public void testCommonTypeForBinaryComparison_stringAndIp_returnsIp() {
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(varcharType, ipUdt);

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isIp(result));
  }

  @Test
  public void testCommonTypeForBinaryComparison_nullTypes_handledGracefully() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType result1 = typeCoercion.commonTypeForBinaryComparison(null, intType);
    RelDataType result2 = typeCoercion.commonTypeForBinaryComparison(intType, null);
    RelDataType result3 = typeCoercion.commonTypeForBinaryComparison(null, null);

    assertNull(result1);
    assertNull(result2);
    assertNull(result3);
  }

  @Test
  public void testCommonTypeForBinaryComparison_preservesNullability() {
    RelDataType nullableDate = TYPE_FACTORY.createSqlType(SqlTypeName.DATE, true);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME, false);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(nullableDate, timeUdt);

    assertNotNull(result);
    // When either type is nullable, result should be nullable
    assertTrue(result.isNullable());
  }

  @Test
  public void testCommonTypeForBinaryComparison_bothNullable_returnsNullable() {
    RelDataType nullableDate = TYPE_FACTORY.createSqlType(SqlTypeName.DATE, true);
    RelDataType nullableTime = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME, true);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(nullableDate, nullableTime);

    assertNotNull(result);
    assertTrue(result.isNullable());
  }

  @Test
  public void testCommonTypeForBinaryComparison_noNullable_returnsNonNull() {
    RelDataType date = TYPE_FACTORY.createSqlType(SqlTypeName.DATE, false);
    RelDataType time = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME, false);

    RelDataType result = typeCoercion.commonTypeForBinaryComparison(date, time);

    assertNotNull(result);
    assertFalse(result.isNullable());
  }
}
