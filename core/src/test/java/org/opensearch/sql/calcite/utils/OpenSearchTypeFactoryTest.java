/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public class OpenSearchTypeFactoryTest {

  @Test
  public void testLeastRestrictivePreservesUdtWhenAllInputsSameUdt() {
    RelDataType ts1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType ts2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ts1, ts2));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_TIMESTAMP, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictivePreservesUdtForDateType() {
    RelDataType d1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RelDataType d2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(d1, d2));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_DATE, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictivePreservesUdtForThreeInputs() {
    RelDataType ts1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType ts2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType ts3 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ts1, ts2, ts3));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_TIMESTAMP, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictiveReturnsNullableWhenAnyInputIsNullable() {
    RelDataType nonNullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, false);
    RelDataType nullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, true);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nonNullable, nullable));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_TIMESTAMP, ((AbstractExprRelDataType<?>) result).getUdt());
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveReturnsNullableWhenFirstNullableSecondNot() {
    RelDataType nullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, true);
    RelDataType nonNullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, false);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullable, nonNullable));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveFallsBackForMixedUdtAndNonUdt() {
    RelDataType udt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType plain = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(udt, plain));

    // Falls back to super.leastRestrictive — both backed by VARCHAR, so result is non-null
    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveFallsBackForDifferentUdts() {
    RelDataType timestamp = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RelDataType date = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(timestamp, date));

    // Different UDTs — falls back to super.leastRestrictive, both backed by VARCHAR
    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveDelegatesToSuperForSingleType() {
    RelDataType single = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(single));

    assertNotNull(result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveDelegatesToSuperForPlainTypes() {
    RelDataType int1 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType int2 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(int1, int2));

    assertNotNull(result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveVarbinaryAndVarcharReturnsVarbinary() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varbinary, varchar));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
    assertFalse(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveVarcharAndVarbinaryReturnsVarbinary() {
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varchar, varbinary));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveVarbinaryAndCharReturnsVarbinary() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType ch = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varbinary, ch));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveVarbinaryAndMultipleVarcharLiteralsReturnsVarbinary() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType v1 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType v2 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varbinary, v1, v2));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveVarbinaryAndNullableVarcharReturnsNullableVarbinary() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType nullableVarchar =
        TYPE_FACTORY.createTypeWithNullability(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varbinary, nullableVarchar));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveNullableVarbinaryAndVarcharReturnsNullableVarbinary() {
    RelDataType nullableVarbinary =
        TYPE_FACTORY.createTypeWithNullability(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY), true);
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullableVarbinary, varchar));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveTwoVarbinariesReturnsVarbinary() {
    RelDataType v1 = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType v2 = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(v1, v2));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveVarbinaryAndIntegerFallsBackToSuper() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    RelDataType integer = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    // Mixing VARBINARY with a non-string type — varbinary/varchar coercion does not apply,
    // so this falls back to super.leastRestrictive (which returns null for incompatible types).
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(varbinary, integer));

    // super.leastRestrictive cannot find a common type for VARBINARY + INTEGER
    // The exact behavior depends on Calcite's type system, but the key check is that
    // we did NOT return VARBINARY from leastRestrictiveVarbinaryVarchar.
    if (result != null) {
      assertFalse(
          result.getSqlTypeName() == SqlTypeName.VARBINARY,
          "VARBINARY + INTEGER should not coerce to VARBINARY via the varbinary/varchar path");
    }
  }

  @Test
  public void testLeastRestrictiveOnlyVarcharsReturnsVarchar() {
    RelDataType v1 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType v2 = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    // No VARBINARY in list — leastRestrictiveVarbinaryVarchar returns null,
    // so super.leastRestrictive resolves to VARCHAR.
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(v1, v2));

    assertNotNull(result);
    assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
  }

  @Test
  public void testConvertSqlTypeNameVarbinaryToBinaryExprType() {
    assertEquals(
        ExprCoreType.BINARY,
        OpenSearchTypeFactory.convertSqlTypeNameToExprType(SqlTypeName.VARBINARY));
  }

  @Test
  public void testConvertSqlTypeNameBinaryToBinaryExprType() {
    assertEquals(
        ExprCoreType.BINARY,
        OpenSearchTypeFactory.convertSqlTypeNameToExprType(SqlTypeName.BINARY));
  }

  @Test
  public void testConvertRelDataTypeVarbinaryToBinaryExprType() {
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    assertEquals(
        ExprCoreType.BINARY, OpenSearchTypeFactory.convertRelDataTypeToExprType(varbinary));
  }

  @Test
  public void testConvertExprTypeBinaryToVarbinaryRelDataType() {
    RelDataType result = OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.BINARY);
    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
  }

  @Test
  public void testConvertExprTypeBinaryToNullableVarbinary() {
    RelDataType result =
        OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.BINARY, true);
    assertNotNull(result);
    assertEquals(SqlTypeName.VARBINARY, result.getSqlTypeName());
    assertTrue(result.isNullable());
  }

  // ---------- convertAnalyticsEngineRelDataTypeToExprType ----------
  // UDT-aware variant for the response-schema path. Must agree with the
  // planner-internal convertRelDataTypeToExprType on every non-UDT input.

  @Test
  public void testConvertAnalyticsEngineIpTypeReturnsIpExprType() {
    ExprType result =
        OpenSearchTypeFactory.convertAnalyticsEngineRelDataTypeToExprType(new IpType(true));
    assertEquals(ExprCoreType.IP, result);
  }

  @Test
  public void testConvertAnalyticsEngineBinaryTypeReturnsBinaryExprType() {
    ExprType result =
        OpenSearchTypeFactory.convertAnalyticsEngineRelDataTypeToExprType(new BinaryType(true));
    assertEquals(ExprCoreType.BINARY, result);
  }

  @Test
  public void testConvertAnalyticsEnginePlainVarbinaryFallsBackToBinary() {
    // Plain VARBINARY (no UDT) must still resolve to BINARY via the delegated path.
    RelDataType varbinary = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
    ExprType result = OpenSearchTypeFactory.convertAnalyticsEngineRelDataTypeToExprType(varbinary);
    assertEquals(ExprCoreType.BINARY, result);
  }

  @Test
  public void testConvertAnalyticsEngineDelegatesParityForNonUdtTypes() {
    // Parity check: drift would mean response-schema labels diverge from Calcite's view.
    RelDataType[] samples =
        new RelDataType[] {
          TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
          TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
          TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN),
          TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE),
          TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP),
        };
    for (RelDataType t : samples) {
      assertEquals(
          OpenSearchTypeFactory.convertRelDataTypeToExprType(t),
          OpenSearchTypeFactory.convertAnalyticsEngineRelDataTypeToExprType(t),
          "Analytics-engine variant must agree with the general variant for " + t.getSqlTypeName());
    }
  }

  @Test
  public void convertExprTypeToRelDataType_timestamp_returns_standard_timestamp9() {
    RelDataType t =
        OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.TIMESTAMP, true);
    assertEquals(SqlTypeName.TIMESTAMP, t.getSqlTypeName());
    assertFalse(OpenSearchTypeFactory.isUserDefinedType(t));
    assertTrue(t.isNullable());
  }

  @Test
  public void convertExprTypeToRelDataType_date_returns_standard_date() {
    RelDataType t = OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.DATE, true);
    assertEquals(SqlTypeName.DATE, t.getSqlTypeName());
    assertFalse(OpenSearchTypeFactory.isUserDefinedType(t));
    assertTrue(t.isNullable());
  }

  @Test
  public void convertExprTypeToRelDataType_time_returns_standard_time9() {
    RelDataType t = OpenSearchTypeFactory.convertExprTypeToRelDataType(ExprCoreType.TIME, true);
    assertEquals(SqlTypeName.TIME, t.getSqlTypeName());
    assertFalse(OpenSearchTypeFactory.isUserDefinedType(t));
    assertTrue(t.isNullable());
  }

  @Test
  public void isStandardTemporalType_recognizes_basic_temporal() {
    assertTrue(
        OpenSearchTypeFactory.isStandardTemporalType(TYPE_FACTORY.createSqlType(SqlTypeName.DATE)));
    assertTrue(
        OpenSearchTypeFactory.isStandardTemporalType(
            TYPE_FACTORY.createSqlType(SqlTypeName.TIME, 9)));
    assertTrue(
        OpenSearchTypeFactory.isStandardTemporalType(
            TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, 9)));
    assertFalse(
        OpenSearchTypeFactory.isStandardTemporalType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)));
    assertFalse(
        OpenSearchTypeFactory.isStandardTemporalType(
            TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP)));
  }
}
