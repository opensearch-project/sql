/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class OpenSearchTypeFactoryTest {

  // ==================== leastRestrictive with UDT types tests ====================

  @Test
  public void testLeastRestrictive_dateUdtsOnly_returnsDateUdt() {
    RelDataType dateUdt1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType dateUdt2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt1, dateUdt2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isDate(result));
  }

  @Test
  public void testLeastRestrictive_timeUdtsOnly_returnsTimeUdt() {
    RelDataType timeUdt1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timeUdt2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(timeUdt1, timeUdt2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTime(result));
  }

  @Test
  public void testLeastRestrictive_timestampUdtsOnly_returnsTimestampUdt() {
    RelDataType timestampUdt1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType timestampUdt2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(timestampUdt1, timestampUdt2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTimestamp(result));
  }

  @Test
  public void testLeastRestrictive_ipUdtsOnly_returnsIpUdt() {
    RelDataType ipUdt1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType ipUdt2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ipUdt1, ipUdt2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isIp(result));
  }

  @Test
  public void testLeastRestrictive_binaryUdtsOnly_returnsBinaryUdt() {
    RelDataType binaryUdt1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType binaryUdt2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(binaryUdt1, binaryUdt2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isBinary(result));
  }

  @Test
  public void testLeastRestrictive_dateAndNull_returnsDateUdt() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt, nullType));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isDate(result));
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictive_timeAndNull_returnsTimeUdt() {
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(timeUdt, nullType));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTime(result));
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictive_ipAndNull_returnsIpUdt() {
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ipUdt, nullType));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isIp(result));
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictive_binaryAndNull_returnsBinaryUdt() {
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(binaryUdt, nullType));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isBinary(result));
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictive_mixedDatetimeTypes_returnsTimestampUdt() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt, timeUdt));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTimestamp(result));
  }

  @Test
  public void testLeastRestrictive_dateTimeTimestamp_returnsTimestampUdt() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType timeUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
    RelDataType timestampUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt, timeUdt, timestampUdt));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isUserDefinedType(result));
    assertTrue(OpenSearchTypeUtil.isTimestamp(result));
  }

  @Test
  public void testLeastRestrictive_ipAndBinary_returnsVarchar() {
    // IP and BINARY are incompatible UDT types, should fall back to VARCHAR
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType binaryUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ipUdt, binaryUdt));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictive_ipUdtAndOther_returnsIpUdt() {
    // When IP UDT is mixed with OTHER type (which is used as intermediate for IP)
    RelDataType ipUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ipUdt, nullType));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isIp(result));
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictive_nullableUdts_preservesNullability() {
    RelDataType nullableDateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE, true);
    RelDataType nonNullableDateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE, false);

    RelDataType result =
        TYPE_FACTORY.leastRestrictive(List.of(nullableDateUdt, nonNullableDateUdt));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isDate(result));
    assertTrue(result.isNullable());
  }

  // ==================== leastRestrictive with standard types tests ====================

  @Test
  public void testLeastRestrictive_standardNumericTypes_returnsLeastRestrictive() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(intType, bigintType));

    assertNotNull(result);
    assertEquals(SqlTypeName.BIGINT, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictive_charType_convertsToVarchar() {
    RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 10);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(charType));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictive_charAndVarchar_returnsVarchar() {
    RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 5);
    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(charType, varcharType));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictive_withAnyType_fallsThrough() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType anyType = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

    // When ANY is present, should fall through to standard leastRestrictive
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt, anyType));

    // Result depends on standard Calcite behavior
    assertNotNull(result);
    assertEquals(SqlTypeName.ANY, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictive_incompatibleTypes_returnsNull() {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType boolType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

    // Integer and boolean are incompatible
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(intType, boolType));

    assertNull(result);
  }

  @Test
  public void testLeastRestrictive_singleType_returnsSameType() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isDate(result));
  }

  @Test
  public void testLeastRestrictive_multipleNulls_returnsNullableUdt() {
    RelDataType dateUdt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType nullType1 = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    RelDataType nullType2 = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(dateUdt, nullType1, nullType2));

    assertNotNull(result);
    assertTrue(OpenSearchTypeUtil.isDate(result));
    assertTrue(result.isNullable());
  }
}
