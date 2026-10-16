/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.type.ExprVectorType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.lang.PPLLangSpec;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

class OpenSearchKnnVectorTypeTest {

  private static OpenSearchKnnVectorType parse(Map<String, Object> fieldMapping) {
    Map<String, OpenSearchDataType> parsed =
        OpenSearchDataType.parseMapping(Map.of("embedding", fieldMapping));
    assertInstanceOf(OpenSearchKnnVectorType.class, parsed.get("embedding"));
    return (OpenSearchKnnVectorType) parsed.get("embedding");
  }

  @Test
  void floatIsTheDefaultDataType() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                768,
                "method",
                Map.of("name", "hnsw", "engine", "faiss", "space_type", "l2")));
    assertAll(
        () -> assertEquals(768, type.getDimension()),
        () -> assertEquals(VectorDataType.FLOAT, type.getDataType()),
        () -> assertEquals("l2", type.getSpaceType()),
        () -> assertEquals("faiss", type.getEngine()),
        () -> assertEquals("hnsw", type.getMethodName()),
        () -> assertNull(type.getModelId()),
        () -> assertEquals(MappingType.KnnVector, type.getMappingType()),
        () -> assertEquals(ExprCoreType.VECTOR, type.getExprCoreType()));
  }

  @Test
  void halfFloat() {
    OpenSearchKnnVectorType type =
        parse(Map.of("type", "knn_vector", "dimension", 8, "data_type", "half_float"));
    assertEquals(VectorDataType.HALF_FLOAT, type.getDataType());
    assertEquals(8, type.getDimension());
  }

  @Test
  void byteVector() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                128,
                "data_type",
                "byte",
                "method",
                Map.of("name", "hnsw", "engine", "lucene", "space_type", "cosinesimil")));
    assertAll(
        () -> assertEquals(VectorDataType.BYTE, type.getDataType()),
        () -> assertEquals(128, type.getDimension()),
        () -> assertEquals("cosinesimil", type.getSpaceType()),
        () -> assertEquals("lucene", type.getEngine()));
  }

  @Test
  void binaryVector() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                1024,
                "data_type",
                "binary",
                "space_type",
                "hamming",
                "method",
                Map.of("name", "hnsw", "engine", "faiss")));
    assertAll(
        () -> assertEquals(VectorDataType.BINARY, type.getDataType()),
        () -> assertEquals(1024, type.getDimension()),
        () -> assertEquals("hamming", type.getSpaceType()));
  }

  @Test
  void spaceTypeAtFieldLevelWinsOverMethod() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                4,
                "space_type",
                "innerproduct",
                "method",
                Map.of("name", "hnsw", "space_type", "l2")));
    assertEquals("innerproduct", type.getSpaceType());
  }

  @Test
  void spaceTypeInsideMethod() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                4,
                "method",
                Map.of("name", "ivf", "engine", "faiss", "space_type", "L2")));
    assertEquals("l2", type.getSpaceType());
    assertEquals("ivf", type.getMethodName());
  }

  @Test
  void absentSpaceTypeAndMethodAreNull() {
    OpenSearchKnnVectorType type = parse(Map.of("type", "knn_vector", "dimension", 3));
    assertAll(
        () -> assertEquals(3, type.getDimension()),
        () -> assertNull(type.getSpaceType()),
        () -> assertNull(type.getEngine()),
        () -> assertNull(type.getMethodName()));
  }

  @Test
  void engineAtFieldLevelWhenNoMethod() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                3,
                "mode",
                "on_disk",
                "engine",
                "faiss",
                "space_type",
                "l2"));
    assertEquals("faiss", type.getEngine());
    assertNull(type.getMethodName());
  }

  @Test
  void modelBasedFieldHasUnknownDimensionAndStaysReadable() {
    OpenSearchKnnVectorType type = parse(Map.of("type", "knn_vector", "model_id", "my-model"));
    assertAll(
        () -> assertEquals(VectorFieldType.UNKNOWN_DIMENSION, type.getDimension()),
        () -> assertEquals("my-model", type.getModelId()),
        () -> assertEquals(VectorDataType.FLOAT, type.getDataType()),
        () -> assertNull(type.getSpaceType()));
    RelDataType relType = OpenSearchTypeFactory.convertExprTypeToRelDataType(type);
    assertInstanceOf(ExprVectorType.class, relType);
    assertEquals("VECTOR(?, FLOAT)", relType.toString());
  }

  @Test
  void dimensionGivenAsStringAndUnknownDataType() {
    OpenSearchKnnVectorType type =
        parse(Map.of("type", "knn_vector", "dimension", "16", "data_type", "int4"));
    assertEquals(16, type.getDimension());
    assertEquals(VectorDataType.FLOAT, type.getDataType());
  }

  /**
   * Only PPL describe with Calcite enabled says vector; the V2 engine and SQL see the ARRAY names
   * of before.
   */
  @Test
  void typeNamesAreVectorOnlyInPplDescribe() {
    OpenSearchKnnVectorType type =
        OpenSearchKnnVectorType.of(4, VectorDataType.BYTE, "L2", null, null);
    assertAll(
        () -> assertEquals("ARRAY", type.typeName()),
        () -> assertEquals("NESTED", type.legacyTypeName()),
        () -> assertEquals("VECTOR", type.describeTypeName(PPLLangSpec.PPL_SPEC, true)),
        () -> assertEquals("ARRAY", type.describeTypeName(PPLLangSpec.PPL_SPEC, false)),
        () -> assertEquals("KNN_VECTOR", type.describeTypeName(LangSpec.SQL_SPEC, true)),
        () -> assertEquals("KNN_VECTOR", type.describeTypeName(LangSpec.SQL_SPEC, false)),
        () -> assertEquals("ARRAY", PPLLangSpec.PPL_SPEC.typeName(type)),
        () -> assertEquals("l2", type.getSpaceType()),
        () -> assertSame(type, type.getExprType()),
        () -> assertEquals(List.of(ExprCoreType.ARRAY), type.getParent()),
        () -> assertFalse(type.shouldCast(ExprCoreType.ARRAY)),
        () -> assertTrue(VectorFieldType.isVector(type)),
        () -> assertTrue(type.isCompatible(type)));
  }

  @Test
  void cloneKeepsAttributesAndEquality() {
    OpenSearchKnnVectorType type =
        parse(
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                768,
                "data_type",
                "byte",
                "method",
                Map.of("name", "hnsw", "engine", "faiss", "space_type", "l2")));
    OpenSearchDataType empty = type.cloneEmpty();
    OpenSearchDataType deep = type.cloneDeep();
    assertAll(
        () -> assertInstanceOf(OpenSearchKnnVectorType.class, empty),
        () -> assertEquals(type, empty),
        () -> assertEquals(type.hashCode(), empty.hashCode()),
        () -> assertEquals(type, deep),
        () -> assertEquals("faiss", ((OpenSearchKnnVectorType) deep).getEngine()));
    assertNotEquals(
        type, OpenSearchKnnVectorType.of(384, VectorDataType.BYTE, "l2", "faiss", "hnsw"));
    assertNotEquals(type, OpenSearchDataType.of(MappingType.Nested));
    assertNotEquals(type, OpenSearchDataType.of(ExprCoreType.VECTOR));
  }

  @Test
  void flattenedMappingKeepsAttributes() {
    Map<String, OpenSearchDataType> parsed =
        OpenSearchDataType.parseMapping(
            Map.of(
                "doc",
                Map.of(
                    "properties",
                    Map.of("embedding", Map.of("type", "knn_vector", "dimension", 2)))));
    Map<String, OpenSearchDataType> flattened = OpenSearchDataType.traverseAndFlatten(parsed);
    OpenSearchDataType embedding = flattened.get("doc.embedding");
    assertInstanceOf(OpenSearchKnnVectorType.class, embedding);
    assertEquals(2, ((OpenSearchKnnVectorType) embedding).getDimension());
  }

  @Test
  void mapsToCalciteVectorType() {
    OpenSearchKnnVectorType type =
        OpenSearchKnnVectorType.of(768, VectorDataType.FLOAT, "l2", "faiss", "hnsw");
    RelDataType relType = OpenSearchTypeFactory.convertExprTypeToRelDataType(type);
    assertInstanceOf(ExprVectorType.class, relType);
    ExprVectorType vector = (ExprVectorType) relType;
    assertAll(
        () -> assertEquals(SqlTypeName.ARRAY, vector.getSqlTypeName()),
        () -> assertEquals(768, vector.getDimension()),
        () -> assertEquals(VectorDataType.FLOAT, vector.getDataType()),
        () -> assertEquals("l2", vector.getSpaceType()),
        () -> assertEquals("VECTOR(768, FLOAT)", vector.toString()));
    assertEquals(
        "VECTOR(16, BINARY)",
        OpenSearchTypeFactory.convertExprTypeToRelDataType(
                OpenSearchKnnVectorType.of(16, VectorDataType.BINARY, null, null, null))
            .toString());
  }

  /** Recording the index keeps the type's identity and its sub-field properties. */
  @Test
  void inIndexKeepsIdentityAndProperties() {
    OpenSearchKnnVectorType type =
        OpenSearchKnnVectorType.of(4, VectorDataType.BYTE, "l2", "faiss", "hnsw");
    OpenSearchKnnVectorType annotated = type.inIndex("i0");
    assertAll(
        () -> assertEquals(type, annotated),
        () -> assertSame(type.getProperties(), annotated.getProperties()),
        () -> assertFalse(annotated.hasMappingMismatch()),
        () -> assertEquals(List.of(), annotated.getMappingMismatchIndices()),
        () -> assertEquals(VectorDataType.BYTE, annotated.getStoredDataType(null)),
        () -> assertEquals(VectorDataType.BYTE, type.getStoredDataType("any")));
  }

  /** A mismatch reaches the Calcite type, which names the indices. */
  @Test
  void mismatchReachesTheCalciteType() {
    OpenSearchKnnVectorType merged =
        OpenSearchKnnVectorType.of(8, VectorDataType.FLOAT, "l2", null, null)
            .inIndex("b")
            .mergedWith(
                OpenSearchKnnVectorType.of(8, VectorDataType.FLOAT, "innerproduct", null, null)
                    .inIndex("a"));
    // Cloning, as flattening the mapping does, keeps the merge.
    OpenSearchKnnVectorType clone = (OpenSearchKnnVectorType) merged.cloneEmpty();
    assertEquals(List.of("a", "b"), clone.getMappingMismatchIndices());
    ExprVectorType vector =
        (ExprVectorType) OpenSearchTypeFactory.convertExprTypeToRelDataType(clone);
    assertAll(
        () -> assertTrue(vector.hasMappingMismatch()),
        () -> assertEquals(List.of("a", "b"), vector.getMappingMismatchIndices()),
        () -> assertEquals(8, vector.getDimension()),
        () -> assertNull(vector.getSpaceType()),
        () -> assertEquals("VECTOR(8, FLOAT)", vector.toString()),
        () -> assertEquals("VECTOR(8, FLOAT, mismatch [a, b])", vector.getFullTypeString()));
  }

  @Test
  void bareMappingTypeHasUnknownDimension() {
    OpenSearchDataType type = OpenSearchDataType.of(MappingType.KnnVector);
    assertInstanceOf(OpenSearchKnnVectorType.class, type);
    assertEquals(
        VectorFieldType.UNKNOWN_DIMENSION, ((OpenSearchKnnVectorType) type).getDimension());
  }
}
