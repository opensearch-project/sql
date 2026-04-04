/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

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
  public void testLeastRestrictiveNullAndIntegerReturnsInteger() {
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullType, intType));

    assertNotNull("leastRestrictive(NULL, INTEGER) should not be null", result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveNullAndDecimalReturnsDecimal() {
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    RelDataType decType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 0);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullType, decType));

    assertNotNull("leastRestrictive(NULL, DECIMAL) should not be null", result);
    assertEquals(SqlTypeName.DECIMAL, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveNullAndBooleanReturnsBoolean() {
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    RelDataType boolType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullType, boolType));

    assertNotNull("leastRestrictive(NULL, BOOLEAN) should not be null", result);
    assertEquals(SqlTypeName.BOOLEAN, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveMultipleNullsAndIntegerReturnsInteger() {
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullType, nullType, intType));

    assertNotNull("leastRestrictive(NULL, NULL, INTEGER) should not be null", result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }
}
