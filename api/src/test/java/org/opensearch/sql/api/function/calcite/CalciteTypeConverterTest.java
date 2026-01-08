/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link CalciteTypeConverter}. */
public class CalciteTypeConverterTest {

  private RelDataTypeFactory typeFactory;

  @Before
  public void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  // ========== Primitive Type Conversion Tests ==========

  @Test
  public void testVarcharConversion() {
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(varcharType);
    assertEquals("VARCHAR", typeName);
  }

  @Test
  public void testCharConversion() {
    RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(charType);
    assertEquals("VARCHAR", typeName);
  }

  @Test
  public void testIntegerConversion() {
    RelDataType integerType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(integerType);
    assertEquals("INTEGER", typeName);
  }

  @Test
  public void testBigintConversion() {
    RelDataType bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(bigintType);
    assertEquals("BIGINT", typeName);
  }

  @Test
  public void testDoubleConversion() {
    RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(doubleType);
    assertEquals("DOUBLE", typeName);
  }

  @Test
  public void testFloatConversion() {
    RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(floatType);
    assertEquals("DOUBLE", typeName);
  }

  @Test
  public void testRealConversion() {
    RelDataType realType = typeFactory.createSqlType(SqlTypeName.REAL);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(realType);
    assertEquals("DOUBLE", typeName);
  }

  @Test
  public void testBooleanConversion() {
    RelDataType booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(booleanType);
    assertEquals("BOOLEAN", typeName);
  }

  @Test
  public void testDateConversion() {
    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(dateType);
    assertEquals("DATE", typeName);
  }

  @Test
  public void testTimestampConversion() {
    RelDataType timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(timestampType);
    assertEquals("TIMESTAMP", typeName);
  }

  @Test
  public void testTimestampWithLocalTimeZoneConversion() {
    RelDataType timestampType =
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(timestampType);
    assertEquals("TIMESTAMP", typeName);
  }

  // ========== Unknown Type Handling Tests ==========

  @Test
  public void testUnknownTypeConversion() {
    RelDataType decimalType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(decimalType);
    assertEquals("UNKNOWN", typeName);
  }

  @Test
  public void testBinaryTypeConversion() {
    RelDataType binaryType = typeFactory.createSqlType(SqlTypeName.BINARY);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(binaryType);
    assertEquals("UNKNOWN", typeName);
  }

  @Test(expected = NullPointerException.class)
  public void testNullRelDataType() {
    CalciteTypeConverter.relDataTypeToSqlTypeName(null);
  }

  // ========== Array Type Conversion Tests ==========

  @Test
  public void testArrayOfIntegerConversion() {
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType arrayType = typeFactory.createArrayType(elementType, -1);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(arrayType);
    assertEquals("ARRAY<INTEGER>", typeName);
  }

  @Test
  public void testArrayOfVarcharConversion() {
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayType = typeFactory.createArrayType(elementType, -1);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(arrayType);
    assertEquals("ARRAY<VARCHAR>", typeName);
  }

  @Test
  public void testArrayOfDoubleConversion() {
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    RelDataType arrayType = typeFactory.createArrayType(elementType, -1);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(arrayType);
    assertEquals("ARRAY<DOUBLE>", typeName);
  }

  @Test
  public void testNestedArrayConversion() {
    RelDataType innerElementType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType innerArrayType = typeFactory.createArrayType(innerElementType, -1);
    RelDataType outerArrayType = typeFactory.createArrayType(innerArrayType, -1);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(outerArrayType);
    assertEquals("ARRAY<ARRAY<INTEGER>>", typeName);
  }

  // ========== Struct Type Conversion Tests ==========

  @Test
  public void testSimpleStructConversion() {
    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType ageType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    RelDataType structType =
        typeFactory.createStructType(
            java.util.List.of(nameType, ageType), java.util.List.of("name", "age"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(structType);
    assertEquals("STRUCT<name:VARCHAR, age:INTEGER>", typeName);
  }

  @Test
  public void testStructWithMultipleFieldsConversion() {
    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType ageType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType salaryType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    RelDataType activeType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    RelDataType structType =
        typeFactory.createStructType(
            java.util.List.of(nameType, ageType, salaryType, activeType),
            java.util.List.of("name", "age", "salary", "active"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(structType);
    assertEquals("STRUCT<name:VARCHAR, age:INTEGER, salary:DOUBLE, active:BOOLEAN>", typeName);
  }

  @Test
  public void testNestedStructConversion() {
    RelDataType streetType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType cityType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType addressType =
        typeFactory.createStructType(
            java.util.List.of(streetType, cityType), java.util.List.of("street", "city"));

    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType personType =
        typeFactory.createStructType(
            java.util.List.of(nameType, addressType), java.util.List.of("name", "address"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(personType);
    assertEquals("STRUCT<name:VARCHAR, address:STRUCT<street:VARCHAR, city:VARCHAR>>", typeName);
  }

  @Test
  public void testEmptyStructConversion() {
    RelDataType emptyStructType =
        typeFactory.createStructType(java.util.List.of(), java.util.List.of());
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(emptyStructType);
    assertEquals("STRUCT<>", typeName);
  }

  @Test
  public void testStructWithArrayFieldConversion() {
    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType scoresType = typeFactory.createArrayType(intType, -1);

    RelDataType structType =
        typeFactory.createStructType(
            java.util.List.of(nameType, scoresType), java.util.List.of("name", "scores"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(structType);
    assertEquals("STRUCT<name:VARCHAR, scores:ARRAY<INTEGER>>", typeName);
  }

  // ========== Reverse Conversion Tests (String to Calcite) ==========

  @Test
  public void testToCalciteTypeVarchar() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("VARCHAR", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.VARCHAR, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeInteger() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("INTEGER", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.INTEGER, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeBigint() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("BIGINT", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.BIGINT, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeDouble() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("DOUBLE", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.DOUBLE, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeBoolean() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("BOOLEAN", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.BOOLEAN, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeDate() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("DATE", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.DATE, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeTimestamp() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("TIMESTAMP", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.TIMESTAMP, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeUnknown() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("UNKNOWN", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.ANY, calciteType.getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeArrayOfInteger() {
    RelDataType calciteType = CalciteTypeConverter.toCalciteType("ARRAY<INTEGER>", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, calciteType.getComponentType().getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeNestedArray() {
    RelDataType calciteType =
        CalciteTypeConverter.toCalciteType("ARRAY<ARRAY<VARCHAR>>", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.ARRAY, calciteType.getSqlTypeName());
    RelDataType innerArray = calciteType.getComponentType();
    assertEquals(SqlTypeName.ARRAY, innerArray.getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, innerArray.getComponentType().getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeSimpleStruct() {
    RelDataType calciteType =
        CalciteTypeConverter.toCalciteType("STRUCT<name:VARCHAR, age:INTEGER>", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.ROW, calciteType.getSqlTypeName());
    assertEquals(2, calciteType.getFieldCount());
    assertEquals("name", calciteType.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.VARCHAR, calciteType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals("age", calciteType.getFieldList().get(1).getName());
    assertEquals(SqlTypeName.INTEGER, calciteType.getFieldList().get(1).getType().getSqlTypeName());
  }

  @Test
  public void testToCalciteTypeNestedStruct() {
    RelDataType calciteType =
        CalciteTypeConverter.toCalciteType(
            "STRUCT<name:VARCHAR, address:STRUCT<street:VARCHAR, city:VARCHAR>>", typeFactory);
    assertNotNull(calciteType);
    assertEquals(SqlTypeName.ROW, calciteType.getSqlTypeName());
    assertEquals(2, calciteType.getFieldCount());

    RelDataType addressType = calciteType.getFieldList().get(1).getType();
    assertEquals(SqlTypeName.ROW, addressType.getSqlTypeName());
    assertEquals(2, addressType.getFieldCount());
    assertEquals("street", addressType.getFieldList().get(0).getName());
    assertEquals("city", addressType.getFieldList().get(1).getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToCalciteTypeEmptyStruct() {
    // Empty struct is not supported in reverse conversion due to regex pattern limitation
    CalciteTypeConverter.toCalciteType("STRUCT<>", typeFactory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToCalciteTypeInvalidTypeName() {
    CalciteTypeConverter.toCalciteType("INVALID_TYPE", typeFactory);
  }

  @Test(expected = NullPointerException.class)
  public void testToCalciteTypeNullTypeName() {
    CalciteTypeConverter.toCalciteType(null, typeFactory);
  }

  @Test(expected = NullPointerException.class)
  public void testToCalciteTypeNullTypeFactory() {
    CalciteTypeConverter.toCalciteType("VARCHAR", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToCalciteTypeInvalidStructFormat() {
    CalciteTypeConverter.toCalciteType("STRUCT<name VARCHAR>", typeFactory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToCalciteTypeEmptyFieldName() {
    CalciteTypeConverter.toCalciteType("STRUCT<:VARCHAR>", typeFactory);
  }

  // ========== Round-Trip Conversion Tests ==========

  @Test
  public void testRoundTripVarchar() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripInteger() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripDouble() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripBoolean() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripDate() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.DATE);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripTimestamp() {
    RelDataType originalType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
  }

  @Test
  public void testRoundTripArrayOfInteger() {
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType originalType = typeFactory.createArrayType(elementType, -1);

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
    assertEquals(
        originalType.getComponentType().getSqlTypeName(),
        reconstructedType.getComponentType().getSqlTypeName());
  }

  @Test
  public void testRoundTripNestedArray() {
    RelDataType innerElementType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType innerArrayType = typeFactory.createArrayType(innerElementType, -1);
    RelDataType originalType = typeFactory.createArrayType(innerArrayType, -1);

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
    assertEquals(
        originalType.getComponentType().getSqlTypeName(),
        reconstructedType.getComponentType().getSqlTypeName());
    assertEquals(
        originalType.getComponentType().getComponentType().getSqlTypeName(),
        reconstructedType.getComponentType().getComponentType().getSqlTypeName());
  }

  @Test
  public void testRoundTripSimpleStruct() {
    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType ageType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType originalType =
        typeFactory.createStructType(
            java.util.List.of(nameType, ageType), java.util.List.of("name", "age"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
    assertEquals(originalType.getFieldCount(), reconstructedType.getFieldCount());
    assertEquals(
        originalType.getFieldList().get(0).getName(),
        reconstructedType.getFieldList().get(0).getName());
    assertEquals(
        originalType.getFieldList().get(0).getType().getSqlTypeName(),
        reconstructedType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(
        originalType.getFieldList().get(1).getName(),
        reconstructedType.getFieldList().get(1).getName());
    assertEquals(
        originalType.getFieldList().get(1).getType().getSqlTypeName(),
        reconstructedType.getFieldList().get(1).getType().getSqlTypeName());
  }

  @Test
  public void testRoundTripNestedStruct() {
    RelDataType streetType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType cityType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType addressType =
        typeFactory.createStructType(
            java.util.List.of(streetType, cityType), java.util.List.of("street", "city"));

    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType originalType =
        typeFactory.createStructType(
            java.util.List.of(nameType, addressType), java.util.List.of("name", "address"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
    assertEquals(originalType.getFieldCount(), reconstructedType.getFieldCount());

    RelDataType originalAddress = originalType.getFieldList().get(1).getType();
    RelDataType reconstructedAddress = reconstructedType.getFieldList().get(1).getType();

    assertEquals(originalAddress.getSqlTypeName(), reconstructedAddress.getSqlTypeName());
    assertEquals(originalAddress.getFieldCount(), reconstructedAddress.getFieldCount());
  }

  @Test
  public void testRoundTripComplexStructWithArrays() {
    RelDataType nameType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType scoresType = typeFactory.createArrayType(intType, -1);

    RelDataType originalType =
        typeFactory.createStructType(
            java.util.List.of(nameType, scoresType), java.util.List.of("name", "scores"));

    String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(originalType);
    RelDataType reconstructedType = CalciteTypeConverter.toCalciteType(typeName, typeFactory);

    assertEquals(originalType.getSqlTypeName(), reconstructedType.getSqlTypeName());
    assertEquals(originalType.getFieldCount(), reconstructedType.getFieldCount());
    assertEquals(
        originalType.getFieldList().get(1).getType().getSqlTypeName(),
        reconstructedType.getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(
        originalType.getFieldList().get(1).getType().getComponentType().getSqlTypeName(),
        reconstructedType.getFieldList().get(1).getType().getComponentType().getSqlTypeName());
  }
}
