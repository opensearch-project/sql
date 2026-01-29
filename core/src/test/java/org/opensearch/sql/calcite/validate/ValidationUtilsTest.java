/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.nio.charset.StandardCharsets;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ValidationUtilsTest {

  @Test
  public void testSyncAttributesNullability() {
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    // Create nullable source type
    RelDataType nullableSource = TYPE_FACTORY.createTypeWithNullability(intType, true);

    // Sync to non-nullable target
    RelDataType synced = ValidationUtils.syncAttributes(TYPE_FACTORY, nullableSource, varchar);

    assertTrue(synced.isNullable());
    assertEquals(SqlTypeName.VARCHAR, synced.getSqlTypeName());
  }

  @Test
  public void testSyncAttributesNonNullableSource() {
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    // Create non-nullable source type
    RelDataType nonNullableSource = TYPE_FACTORY.createTypeWithNullability(intType, false);

    // Sync to target
    RelDataType synced = ValidationUtils.syncAttributes(TYPE_FACTORY, nonNullableSource, varchar);

    assertFalse(synced.isNullable());
    assertEquals(SqlTypeName.VARCHAR, synced.getSqlTypeName());
  }

  @Test
  public void testSyncAttributesCharsetAndCollationForCharTypes() {
    // Create source varchar with charset and collation
    RelDataType sourceVarchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    sourceVarchar =
        TYPE_FACTORY.createTypeWithCharsetAndCollation(
            sourceVarchar, StandardCharsets.UTF_8, SqlCollation.IMPLICIT);
    sourceVarchar = TYPE_FACTORY.createTypeWithNullability(sourceVarchar, true);

    RelDataType targetChar = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 10);

    RelDataType synced = ValidationUtils.syncAttributes(TYPE_FACTORY, sourceVarchar, targetChar);

    assertTrue(synced.isNullable());
    assertEquals(StandardCharsets.UTF_8, synced.getCharset());
    assertNotNull(synced.getCollation());
  }

  @Test
  public void testSyncAttributesWithNullFromType() {
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    // When fromType is null, toType should be returned as-is
    RelDataType synced = ValidationUtils.syncAttributes(TYPE_FACTORY, null, varchar);

    assertEquals(varchar, synced);
    assertEquals(SqlTypeName.VARCHAR, synced.getSqlTypeName());
  }

  @Test
  public void testSyncAttributesNonCharTypes() {
    // Test with numeric types - should only sync nullability, not charset/collation
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType doubleType = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);

    RelDataType nullableInt = TYPE_FACTORY.createTypeWithNullability(intType, true);

    RelDataType synced = ValidationUtils.syncAttributes(TYPE_FACTORY, nullableInt, doubleType);

    assertTrue(synced.isNullable());
    assertEquals(SqlTypeName.DOUBLE, synced.getSqlTypeName());
  }

  @Test
  public void testCreateUDTWithAttributesExprUDTDate() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);

    RelDataType dateUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, ExprUDT.EXPR_DATE);

    assertNotNull(dateUdt);
    assertTrue(dateUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesExprUDTTime() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, false);

    RelDataType timeUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, ExprUDT.EXPR_TIME);

    assertNotNull(timeUdt);
    assertFalse(timeUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesExprUDTTimestamp() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);

    RelDataType timestampUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, ExprUDT.EXPR_TIMESTAMP);

    assertNotNull(timestampUdt);
    assertTrue(timestampUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesExprUDTBinary() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY, true);

    RelDataType binaryUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, ExprUDT.EXPR_BINARY);

    assertNotNull(binaryUdt);
    assertTrue(binaryUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesInvalidFactory() {
    // Create a non-OpenSearchTypeFactory
    RelDataTypeFactory basicFactory =
        new org.apache.calcite.jdbc.JavaTypeFactoryImpl(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    RelDataType sourceType = basicFactory.createSqlType(SqlTypeName.VARCHAR);

    assertThrows(
        IllegalArgumentException.class,
        () -> ValidationUtils.createUDTWithAttributes(basicFactory, sourceType, ExprUDT.EXPR_DATE));
  }

  @Test
  public void testCreateUDTWithAttributesSqlTypeNameDate() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);

    RelDataType dateUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, SqlTypeName.DATE);

    assertNotNull(dateUdt);
    assertTrue(dateUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesSqlTypeNameTime() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, false);

    RelDataType timeUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, SqlTypeName.TIME);

    assertNotNull(timeUdt);
    assertFalse(timeUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesSqlTypeNameTimestamp() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);

    RelDataType timestampUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, SqlTypeName.TIMESTAMP);

    assertNotNull(timestampUdt);
    assertTrue(timestampUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesSqlTypeNameBinary() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY, true);

    RelDataType binaryUdt =
        ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, SqlTypeName.BINARY);

    assertNotNull(binaryUdt);
    assertTrue(binaryUdt.isNullable());
  }

  @Test
  public void testCreateUDTWithAttributesUnsupportedSqlTypeName() {
    RelDataType sourceType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, true);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ValidationUtils.createUDTWithAttributes(TYPE_FACTORY, sourceType, SqlTypeName.INTEGER));
  }

  @Test
  public void testTolerantValidationExceptionNestedAggregate() {
    Exception e = new RuntimeException("Aggregate expressions cannot be nested");
    assertTrue(ValidationUtils.tolerantValidationException(e));
  }

  @Test
  public void testTolerantValidationExceptionWindowedInGroupBy() {
    Exception e =
        new RuntimeException("Windowed aggregate expression is illegal in GROUP BY clause");
    assertTrue(ValidationUtils.tolerantValidationException(e));
  }

  @Test
  public void testTolerantValidationExceptionNonMatchingMessage() {
    Exception e = new RuntimeException("Some other error message");
    assertFalse(ValidationUtils.tolerantValidationException(e));
  }

  @Test
  public void testTolerantValidationExceptionNullMessage() {
    Exception e = new RuntimeException();
    assertFalse(ValidationUtils.tolerantValidationException(e));
  }
}
