/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.RelToSqlConverterUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.exception.SemanticCheckException;

class ExprVectorTypeTest {

  private static ExprVectorType vector(int dimension, VectorDataType dataType) {
    return TYPE_FACTORY.createVectorType(dimension, dataType, "l2", true);
  }

  @Test
  void isAnArrayWithVectorAttributes() {
    ExprVectorType type = TYPE_FACTORY.createVectorType(768, VectorDataType.FLOAT, "l2", true);
    assertAll(
        () -> assertInstanceOf(ArraySqlType.class, type),
        () -> assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName()),
        () -> assertEquals(SqlTypeName.REAL, type.getComponentType().getSqlTypeName()),
        () -> assertEquals(768, type.getDimension()),
        () -> assertEquals(768, type.getElementCount()),
        () -> assertEquals(VectorDataType.FLOAT, type.getDataType()),
        () -> assertEquals("l2", type.getSpaceType()),
        () -> assertEquals(ExprUDT.EXPR_VECTOR, type.getUdt()),
        () -> assertEquals(ExprCoreType.VECTOR, type.getExprType()),
        () -> assertTrue(type.isNullable()),
        () -> assertTrue(OpenSearchTypeFactory.isVectorType(type)));
  }

  @ParameterizedTest
  @EnumSource(VectorDataType.class)
  void elementTypeFollowsDataType(VectorDataType dataType) {
    ExprVectorType type = vector(16, dataType);
    SqlTypeName expected = dataType.hasByteElements() ? SqlTypeName.TINYINT : SqlTypeName.REAL;
    assertEquals(expected, type.getComponentType().getSqlTypeName());
    assertEquals(dataType == VectorDataType.BINARY ? 2 : 16, type.getElementCount());
  }

  @Test
  void binaryDimensionCountsBits() {
    ExprVectorType type = vector(1024, VectorDataType.BINARY);
    assertAll(
        () -> assertEquals(1024, type.getDimension()),
        () -> assertEquals(128, type.getElementCount()),
        () -> assertEquals("VECTOR(1024, BINARY)", type.toString()));
  }

  @Test
  void toStringShowsDimensionAndDataType() {
    assertEquals("VECTOR(768, FLOAT)", vector(768, VectorDataType.FLOAT).toString());
    assertEquals("VECTOR(128, BYTE)", vector(128, VectorDataType.BYTE).toString());
    assertEquals("VECTOR(4, HALF_FLOAT)", vector(4, VectorDataType.HALF_FLOAT).toString());
  }

  @Test
  void digestAddsSpaceTypeAndNullability() {
    assertEquals("VECTOR(768, FLOAT, l2)", vector(768, VectorDataType.FLOAT).getFullTypeString());
    assertEquals(
        "VECTOR(768, FLOAT) NOT NULL",
        TYPE_FACTORY.createVectorType(768, VectorDataType.FLOAT, null, false).getFullTypeString());
  }

  @Test
  void unknownDimensionPrintsQuestionMark() {
    ExprVectorType type =
        TYPE_FACTORY.createVectorType(
            ExprVectorType.UNKNOWN_DIMENSION, VectorDataType.FLOAT, null, true);
    assertAll(
        () -> assertEquals("VECTOR(?, FLOAT)", type.toString()),
        () -> assertFalse(type.hasKnownDimension()),
        () -> assertEquals(ExprVectorType.UNKNOWN_DIMENSION, type.getElementCount()));
  }

  @Test
  void equalityIncludesDimensionDataTypeAndSpaceType() {
    assertSame(vector(768, VectorDataType.FLOAT), vector(768, VectorDataType.FLOAT));
    assertNotEquals(vector(768, VectorDataType.FLOAT), vector(384, VectorDataType.FLOAT));
    assertNotEquals(vector(8, VectorDataType.FLOAT), vector(8, VectorDataType.HALF_FLOAT));
    assertNotEquals(vector(8, VectorDataType.BYTE), vector(8, VectorDataType.BINARY));
    // Canonization must not hand back a type with another space type.
    ExprVectorType cosine =
        TYPE_FACTORY.createVectorType(768, VectorDataType.FLOAT, "cosinesimil", true);
    assertNotEquals(vector(768, VectorDataType.FLOAT), cosine);
    assertEquals("cosinesimil", cosine.getSpaceType());
    // Never equal to the plain array of the same element type.
    RelDataType plainArray =
        TYPE_FACTORY.createTypeWithNullability(
            TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.REAL, true), -1),
            true);
    assertNotEquals(plainArray, vector(768, VectorDataType.FLOAT));
    assertFalse(OpenSearchTypeFactory.isVectorType(plainArray));
  }

  @Test
  void deepEqualsComparesAttributes() {
    ExprVectorType a = vector(4, VectorDataType.FLOAT);
    ExprVectorType sameAsA = a.withNullability(true);
    assertTrue(a.deepEquals(sameAsA));
    assertEquals(a.deepHashCode(), sameAsA.deepHashCode());
    assertFalse(a.deepEquals(vector(5, VectorDataType.FLOAT)));
    assertFalse(a.deepEquals(a.withSpaceType("innerproduct")));
    assertFalse(a.deepEquals(a.withNullability(false)));
  }

  @Test
  void createTypeWithNullabilityKeepsVectorAttributes() {
    ExprVectorType nullable = vector(768, VectorDataType.BYTE);
    RelDataType notNull = TYPE_FACTORY.createTypeWithNullability(nullable, false);
    assertInstanceOf(ExprVectorType.class, notNull);
    ExprVectorType vector = (ExprVectorType) notNull;
    assertAll(
        () -> assertFalse(vector.isNullable()),
        () -> assertEquals(768, vector.getDimension()),
        () -> assertEquals(VectorDataType.BYTE, vector.getDataType()),
        () -> assertEquals("l2", vector.getSpaceType()),
        () -> assertSame(nullable, TYPE_FACTORY.createTypeWithNullability(vector, true)),
        () -> assertSame(nullable, TYPE_FACTORY.createTypeWithNullability(nullable, true)));
  }

  @Test
  void enforceTypeWithNullabilityKeepsVectorAttributes() {
    ExprVectorType nullable = vector(768, VectorDataType.FLOAT);
    RelDataType notNull = TYPE_FACTORY.enforceTypeWithNullability(nullable, false);
    assertInstanceOf(ExprVectorType.class, notNull);
    assertFalse(notNull.isNullable());
    assertEquals(768, ((ExprVectorType) notNull).getDimension());
    assertSame(nullable, TYPE_FACTORY.enforceTypeWithNullability(notNull, true));
    assertSame(nullable, TYPE_FACTORY.enforceTypeWithNullability(nullable, true));
  }

  /** {@code RexBuilder.makeFieldAccess} on a nullable row enforces a nullable field type. */
  @Test
  void fieldAccessOnNullableRowKeepsVectorType() {
    RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
    ExprVectorType notNull = TYPE_FACTORY.createVectorType(3, VectorDataType.BYTE, null, false);
    RelDataType row =
        TYPE_FACTORY.createStructType(
            List.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), notNull),
            List.of("id", "v"),
            true);
    RexNode access = rexBuilder.makeFieldAccess(rexBuilder.makeInputRef(row, 0), "v", true);
    assertInstanceOf(ExprVectorType.class, access.getType());
    assertTrue(access.getType().isNullable());
    assertEquals("VECTOR(3, BYTE)", access.getType().toString());
  }

  @Test
  void toRuntimeValueConvertsStoredNumbers() {
    assertEquals(
        List.of(1.0f, 2.5f, -0.006929283495992422f),
        vector(3, VectorDataType.FLOAT).toRuntimeValue(List.of(1, 2.5d, -0.006929283495992422d)));
    assertEquals(
        List.of((byte) -128, (byte) 7),
        vector(2, VectorDataType.BYTE).toRuntimeValue(List.of(-128L, 7)));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue(null));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue(List.of()));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue(List.of(1, "x")));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue("1,2"));
  }

  /** The base64 and single-number forms the k-NN plugin accepts, decoded as it decodes them. */
  @Test
  void toRuntimeValueDecodesBase64AndSingleNumbers() {
    // 1.0f, -2.5f as little-endian float32
    assertEquals(
        List.of(1.0f, -2.5f), vector(2, VectorDataType.FLOAT).toRuntimeValue("AACAPwAAIMA="));
    assertEquals(
        List.of(1.0f, -2.5f), vector(2, VectorDataType.HALF_FLOAT).toRuntimeValue("AACAPwAAIMA="));
    // bytes 1 and -1
    assertEquals(
        List.of((byte) 1, (byte) -1), vector(2, VectorDataType.BYTE).toRuntimeValue("Af8="));
    assertEquals(
        List.of((byte) 1, (byte) -1), vector(16, VectorDataType.BINARY).toRuntimeValue("Af8="));
    // 3 bytes is not a whole float32
    assertNull(vector(1, VectorDataType.FLOAT).toRuntimeValue("Af8B"));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue("not base64!"));
    assertNull(vector(2, VectorDataType.FLOAT).toRuntimeValue(""));
    assertEquals(List.of(7.0f), vector(1, VectorDataType.FLOAT).toRuntimeValue(7));
    assertEquals(List.of((byte) 7), vector(1, VectorDataType.BYTE).toRuntimeValue(7.0d));
  }

  /**
   * A value indexed with another element encoding than its column has, as under a column that
   * merges a float index and a byte index: decoded as stored, then converted to the column's type.
   */
  @Test
  void toRuntimeValueDecodesWithTheStoredEncoding() {
    assertEquals(
        List.of(1.0f, 2.0f, 3.0f, -1.0f), ExprVectorType.toRuntimeValue("AQID/w==", true, false));
    assertEquals(List.of((byte) 1), ExprVectorType.toRuntimeValue("AACAPw==", false, true));
    assertEquals(List.of(1.0f, 2.0f), ExprVectorType.toRuntimeValue(List.of(1, 2), true, false));
    assertNull(ExprVectorType.toRuntimeValue("Af8B", false, false));
  }

  /** A mapping mismatch is part of the type: it is in the digest and survives nullability. */
  @Test
  void mappingMismatchIsPartOfTheType() {
    ExprVectorType mismatch =
        merged(3, new ExprVectorType.IndexMappings(true, List.of("a", "b"), List.of()));
    ExprVectorType plain = TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, null, true);
    assertAll(
        () -> assertTrue(mismatch.hasMappingMismatch()),
        () -> assertEquals(List.of("a", "b"), mismatch.getMappingMismatchIndices()),
        () -> assertFalse(plain.hasMappingMismatch()),
        () -> assertEquals(List.of(), plain.getMappingMismatchIndices()),
        () -> assertEquals("VECTOR(3, FLOAT)", mismatch.toString()),
        () -> assertEquals("VECTOR(3, FLOAT, mismatch [a, b])", mismatch.getFullTypeString()),
        () -> assertNotEquals(plain, mismatch),
        () -> assertFalse(plain.deepEquals(mismatch)));
    RelDataType notNull = TYPE_FACTORY.createTypeWithNullability(mismatch, false);
    assertInstanceOf(ExprVectorType.class, notNull);
    assertFalse(notNull.isNullable());
    assertEquals(List.of("a", "b"), ((ExprVectorType) notNull).getMappingMismatchIndices());
  }

  private static ExprVectorType merged(int dimension, ExprVectorType.IndexMappings mappings) {
    return (ExprVectorType)
        TYPE_FACTORY.createTypeWithNullability(
            new ExprVectorType(
                TYPE_FACTORY.createSqlType(SqlTypeName.REAL, true),
                true,
                dimension,
                VectorDataType.FLOAT,
                null,
                mappings),
            true);
  }

  /** Index a maps the field with float, index b with byte: a base64 value needs its index. */
  private static final ExprVectorType.IndexMappings MIXED =
      new ExprVectorType.IndexMappings(true, List.of("a", "b"), List.of("b"));

  @Test
  void mixedElementEncodingsTellEachIndexEncoding() {
    ExprVectorType mixed = merged(-1, MIXED);
    assertAll(
        () -> assertTrue(mixed.hasMixedElementEncodings()),
        () -> assertEquals(false, mixed.storesByteElements("a")),
        () -> assertEquals(true, mixed.storesByteElements("b")),
        () -> assertNull(mixed.storesByteElements(null)),
        () -> assertNull(mixed.storesByteElements("c")),
        () ->
            assertEquals(
                "VECTOR(?, FLOAT, mismatch [a, b], byte elements in [b])",
                mixed.getFullTypeString()));
    ExprVectorType plain = vector(3, VectorDataType.BYTE);
    assertFalse(plain.hasMixedElementEncodings());
    assertEquals(true, plain.storesByteElements(null));
    assertEquals(false, vector(3, VectorDataType.FLOAT).storesByteElements("any"));
  }

  /** The facts no merge gives are rejected. */
  @Test
  void indexMappingsRejectInconsistentFacts() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExprVectorType.IndexMappings(false, List.of("a"), List.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExprVectorType.IndexMappings(true, List.of("a", "b"), List.of("c")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExprVectorType.IndexMappings(true, List.of("a", "b"), List.of("a", "b")));
    assertEquals(
        List.of("a", "b"),
        List.copyOf(
            new ExprVectorType.IndexMappings(true, List.of("b", "a"), List.of()).indices()));
  }

  /** Combining a merged vector with another keeps what either knows, in any order. */
  @Test
  void leastRestrictiveCombinesIndexMappingsInAnyOrder() {
    ExprVectorType mismatch =
        merged(3, new ExprVectorType.IndexMappings(true, List.of("a", "b"), List.of()));
    ExprVectorType other =
        merged(3, new ExprVectorType.IndexMappings(true, List.of("c", "d"), List.of()));
    ExprVectorType plain = TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, null, true);
    RelDataType first = TYPE_FACTORY.leastRestrictive(List.of(mismatch, plain));
    RelDataType second = TYPE_FACTORY.leastRestrictive(List.of(plain, mismatch));
    assertEquals(first, second);
    assertEquals(List.of("a", "b"), ((ExprVectorType) first).getMappingMismatchIndices());
    assertEquals(
        List.of("a", "b", "c", "d"),
        ((ExprVectorType) TYPE_FACTORY.leastRestrictive(List.of(other, plain, mismatch)))
            .getMappingMismatchIndices());
    ExprVectorType mixed = merged(-1, MIXED);
    ExprVectorType unknownPlain =
        TYPE_FACTORY.createVectorType(-1, VectorDataType.FLOAT, null, true);
    assertTrue(
        ((ExprVectorType) TYPE_FACTORY.leastRestrictive(List.of(unknownPlain, mixed)))
            .hasMixedElementEncodings());
  }

  @Test
  void isNotComparable() {
    assertEquals(RelDataTypeComparability.NONE, vector(4, VectorDataType.FLOAT).getComparability());
  }

  @Test
  void rejectsElementTypeThatDoesNotMatchDataType() {
    RelDataType real = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
    assertThrows(
        IllegalArgumentException.class,
        () -> new ExprVectorType(real, true, 8, VectorDataType.BYTE, null));
  }

  @Test
  void leastRestrictiveKeepsSameVectorTypeAndMergesNullability() {
    ExprVectorType notNull = TYPE_FACTORY.createVectorType(768, VectorDataType.FLOAT, "l2", false);
    ExprVectorType nullable = vector(768, VectorDataType.FLOAT);
    assertSame(notNull, TYPE_FACTORY.leastRestrictive(List.of(notNull, notNull)));
    assertSame(nullable, TYPE_FACTORY.leastRestrictive(List.of(notNull, nullable)));
    assertSame(nullable, TYPE_FACTORY.leastRestrictive(List.of(nullable, notNull)));
    RelDataType nullType = TYPE_FACTORY.createSqlType(SqlTypeName.NULL);
    assertSame(nullable, TYPE_FACTORY.leastRestrictive(List.of(nullType, notNull)));
  }

  @Test
  void leastRestrictiveForgetsSpaceTypeWhenInputsDisagree() {
    ExprVectorType l2 = vector(768, VectorDataType.FLOAT);
    ExprVectorType cosine =
        TYPE_FACTORY.createVectorType(768, VectorDataType.FLOAT, "cosinesimil", true);
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(l2, cosine));
    assertInstanceOf(ExprVectorType.class, result);
    assertNull(((ExprVectorType) result).getSpaceType());
    assertEquals(768, ((ExprVectorType) result).getDimension());
  }

  @Test
  void leastRestrictiveRejectsMixedDimensions() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                TYPE_FACTORY.leastRestrictive(
                    List.of(vector(768, VectorDataType.FLOAT), vector(384, VectorDataType.FLOAT))));
    assertEquals("cannot combine VECTOR(768, FLOAT) and VECTOR(384, FLOAT)", e.getMessage());
  }

  @Test
  void leastRestrictiveRejectsMixedDataTypes() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                TYPE_FACTORY.leastRestrictive(
                    List.of(vector(8, VectorDataType.FLOAT), vector(8, VectorDataType.BYTE))));
    assertEquals("cannot combine VECTOR(8, FLOAT) and VECTOR(8, BYTE)", e.getMessage());
  }

  @Test
  void leastRestrictiveRejectsVectorAndNonVector() {
    RelDataType integer = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType plainArray =
        TYPE_FACTORY.createArrayType(TYPE_FACTORY.createSqlType(SqlTypeName.REAL), -1);
    ExprVectorType vector = vector(8, VectorDataType.FLOAT);
    assertThrows(
        SemanticCheckException.class,
        () -> TYPE_FACTORY.leastRestrictive(List.of(integer, vector)));
    assertThrows(
        SemanticCheckException.class,
        () -> TYPE_FACTORY.leastRestrictive(List.of(vector, plainArray)));
    // Also through the two-argument variant Calcite calls internally.
    assertThrows(
        SemanticCheckException.class,
        () ->
            TYPE_FACTORY.leastRestrictive(
                List.of(vector, plainArray),
                org.apache.calcite.sql.type.SqlTypeMappingRules.instance(false)));
  }

  @Test
  void leastRestrictiveOfRowsKeepsVectorColumn() {
    ExprVectorType vector = vector(8, VectorDataType.FLOAT);
    RelDataType row =
        TYPE_FACTORY.createStructType(
            List.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), vector), List.of("id", "v"));
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(row, row));
    assertSame(vector, result.getFieldList().get(1).getType());

    RelDataType otherRow =
        TYPE_FACTORY.createStructType(
            List.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), vector(9, VectorDataType.FLOAT)),
            List.of("id", "v"));
    assertThrows(
        SemanticCheckException.class, () -> TYPE_FACTORY.leastRestrictive(List.of(row, otherRow)));
  }

  /** {@code RexBuilder.makeLiteral} casts any ARRAY type to ArraySqlType. */
  @Test
  void rexBuilderMakesVectorLiteral() {
    RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
    ExprVectorType vector = vector(3, VectorDataType.FLOAT);
    RexNode literal = rexBuilder.makeLiteral(List.of(1.0f, 2.0f, 3.0f), vector, true);
    assertSame(vector, literal.getType());
    // The ARRAY[...] constructor gets a plain array type, which is then cast to the vector type.
    assertEquals(SqlKind.CAST, literal.getKind());
    RexNode array = ((RexCall) literal).getOperands().getFirst();
    assertEquals(SqlKind.ARRAY_VALUE_CONSTRUCTOR, array.getKind());
    assertEquals(3, ((RexCall) array).getOperands().size());
  }

  @Test
  void rexBuilderMakesNullVectorLiteralOfTheVectorType() {
    RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
    ExprVectorType vector = vector(3, VectorDataType.FLOAT);
    assertSame(vector, rexBuilder.makeNullLiteral(vector).getType());
  }

  /** {@code RelToSqlConverterUtil} casts any ARRAY type to ArraySqlType for dialect cast specs. */
  @Test
  void relToSqlCastSpecAcceptsVector() {
    SqlDataTypeSpec spec =
        RelToSqlConverterUtil.getCastSpecAngleBracketArrayType(
            SparkSqlDialect.DEFAULT, vector(3, VectorDataType.FLOAT), SqlParserPos.ZERO);
    assertEquals("ARRAY< REAL >", spec.toString());
    SqlDataTypeSpec clickHouseSpec =
        RelToSqlConverterUtil.getCastSpecClickHouseSqlArrayType(
            SparkSqlDialect.DEFAULT, vector(3, VectorDataType.BYTE), SqlParserPos.ZERO);
    assertTrue(clickHouseSpec.toString().contains("TINYINT"), clickHouseSpec.toString());
  }

  @Test
  void javaClassIsList() {
    assertEquals(List.class, TYPE_FACTORY.getJavaClass(vector(3, VectorDataType.FLOAT)));
  }

  @Test
  void createUdtGivesVectorOfUnknownDimension() {
    RelDataType type = TYPE_FACTORY.createUDT(ExprUDT.EXPR_VECTOR, true);
    assertInstanceOf(ExprVectorType.class, type);
    assertEquals("VECTOR(?, FLOAT)", type.toString());
    assertTrue(type.isNullable());
  }
}
