/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;

/** Multi-index mapping merge of knn_vector fields. */
class VectorMergeRuleTest {

  private static OpenSearchKnnVectorType knn(int dimension, VectorDataType dataType, String space) {
    return OpenSearchKnnVectorType.of(dimension, dataType, space, "faiss", "hnsw");
  }

  /**
   * The merged type of the field after merging the given types in order, as DESCRIBE does: the i-th
   * type is index {@code i<i>}'s mapping.
   */
  private static OpenSearchDataType merge(OpenSearchDataType... types) {
    Map<String, OpenSearchDataType> merged = new LinkedHashMap<>();
    for (int i = 0; i < types.length; i++) {
      OpenSearchDataType copy = types[i].cloneDeep();
      Map<String, OpenSearchDataType> index = new LinkedHashMap<>();
      index.put(
          "f", copy instanceof OpenSearchKnnVectorType vector ? vector.inIndex("i" + i) : copy);
      MergeRuleHelper.merge(merged, index);
    }
    return merged.get("f");
  }

  private static OpenSearchKnnVectorType mergeVectors(OpenSearchDataType... types) {
    return (OpenSearchKnnVectorType) merge(types);
  }

  @Test
  void agreeingMappingsKeepTheFirstIndexType() {
    OpenSearchKnnVectorType merged =
        mergeVectors(knn(768, VectorDataType.FLOAT, "l2"), knn(768, VectorDataType.FLOAT, "l2"));
    assertEquals(knn(768, VectorDataType.FLOAT, "l2"), merged);
    assertFalse(merged.hasMappingMismatch());
    assertEquals(List.of(), merged.getMappingMismatchIndices());
    assertEquals(VectorDataType.FLOAT, merged.getStoredDataType(null));
  }

  @Test
  void differentDimensionsGiveAnUnknownDimension() {
    OpenSearchKnnVectorType merged =
        mergeVectors(knn(768, VectorDataType.FLOAT, "l2"), knn(384, VectorDataType.FLOAT, "l2"));
    assertAll(
        () -> assertTrue(merged.hasMappingMismatch()),
        () -> assertEquals(List.of("i0", "i1"), merged.getMappingMismatchIndices()),
        () -> assertEquals(VectorFieldType.UNKNOWN_DIMENSION, merged.getDimension()),
        () -> assertEquals(VectorDataType.FLOAT, merged.getDataType()),
        () -> assertEquals("l2", merged.getSpaceType()),
        () -> assertEquals("faiss", merged.getEngine()),
        () ->
            assertEquals(
                "VECTOR(?, FLOAT)",
                OpenSearchTypeFactory.convertExprTypeToRelDataType(merged).toString()));
    // The order of the indices does not matter, and a third agreeing index keeps the mismatch.
    OpenSearchKnnVectorType reordered =
        mergeVectors(
            knn(384, VectorDataType.FLOAT, "l2"),
            knn(768, VectorDataType.FLOAT, "l2"),
            knn(768, VectorDataType.FLOAT, "l2"));
    assertEquals(merged, reordered);
    assertEquals(List.of("i0", "i1", "i2"), reordered.getMappingMismatchIndices());
  }

  /** A field mapping as the cluster returns it, for field {@code f}. */
  private static OpenSearchKnnVectorType parsed(Map<String, Object> mapping) {
    return (OpenSearchKnnVectorType) OpenSearchDataType.parseMapping(Map.of("f", mapping)).get("f");
  }

  private static final Map<String, Object> PLAIN = Map.of("type", "knn_vector", "dimension", 3);

  /**
   * How the k-NN plugin returns {@code "method": {"name": "hnsw"}}: with the space type, engine and
   * parameters it resolved (k-NN {@code KNNMethodContext.toXContent} always writes {@code
   * space_type}).
   */
  private static Map<String, Object> resolvedMethod(String spaceType) {
    return Map.of(
        "type",
        "knn_vector",
        "dimension",
        3,
        "method",
        Map.of("engine", "faiss", "space_type", spaceType, "name", "hnsw", "parameters", Map.of()));
  }

  static Stream<Arguments> spaceTypes() {
    return Stream.of(
        // An undeclared space type is the k-NN default, l2 (hamming for binary).
        args("plain and method l2", PLAIN, resolvedMethod("l2"), false),
        args(
            "plain and top-level l2",
            PLAIN,
            Map.of("type", "knn_vector", "dimension", 3, "space_type", "l2"),
            false),
        args(
            "plain and top-level L2",
            PLAIN,
            Map.of("type", "knn_vector", "dimension", 3, "space_type", "L2"),
            false),
        args(
            "plain and undefined",
            PLAIN,
            Map.of("type", "knn_vector", "dimension", 3, "space_type", "undefined"),
            false),
        args(
            "binary plain and method hamming",
            Map.of("type", "knn_vector", "dimension", 8, "data_type", "BINARY"),
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                8,
                "data_type",
                "BINARY",
                "method",
                Map.of("engine", "faiss", "space_type", "hamming", "name", "hnsw")),
            false),
        // Engine, method name and parameters do not matter.
        args(
            "hnsw and ivf",
            resolvedMethod("l2"),
            Map.of(
                "type",
                "knn_vector",
                "dimension",
                3,
                "method",
                Map.of("engine", "lucene", "space_type", "l2", "name", "ivf")),
            false),
        args("plain and method innerproduct", PLAIN, resolvedMethod("innerproduct"), true),
        args(
            "method l2 and cosinesimil", resolvedMethod("l2"), resolvedMethod("cosinesimil"), true),
        // A model-based field's space type is the model's: the same model agrees.
        args(
            "same model",
            Map.of("type", "knn_vector", "model_id", "m1"),
            Map.of("type", "knn_vector", "model_id", "m1"),
            false),
        args(
            "different models",
            Map.of("type", "knn_vector", "model_id", "m1"),
            Map.of("type", "knn_vector", "model_id", "m2"),
            true));
  }

  private static Arguments args(
      String name, Map<String, Object> first, Map<String, Object> second, boolean mismatch) {
    return Arguments.of(name, first, second, mismatch);
  }

  /** A space type is compared as the k-NN plugin resolves it, in either order. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("spaceTypes")
  void spaceTypeIsComparedAsResolved(
      String name, Map<String, Object> first, Map<String, Object> second, boolean mismatch) {
    assertEquals(mismatch, mergeVectors(parsed(first), parsed(second)).hasMappingMismatch());
    assertEquals(mismatch, mergeVectors(parsed(second), parsed(first)).hasMappingMismatch());
  }

  /** Mappings that agree on the resolved space type keep it, also when one does not declare it. */
  @Test
  void resolvedSpaceTypeIsKept() {
    assertEquals("l2", mergeVectors(parsed(PLAIN), parsed(resolvedMethod("l2"))).getSpaceType());
    assertNull(mergeVectors(parsed(PLAIN), parsed(PLAIN)).getSpaceType());
    assertNull(mergeVectors(parsed(PLAIN), parsed(resolvedMethod("innerproduct"))).getSpaceType());
  }

  @Test
  void differentSpaceTypesKeepTheDimension() {
    OpenSearchKnnVectorType merged =
        mergeVectors(
            knn(8, VectorDataType.FLOAT, "l2"), knn(8, VectorDataType.FLOAT, "cosinesimil"));
    assertEquals(8, merged.getDimension());
    assertNull(merged.getSpaceType());
    assertTrue(merged.hasMappingMismatch());
  }

  @Test
  void differentDataTypes() {
    OpenSearchKnnVectorType floatAndByte =
        mergeVectors(knn(8, VectorDataType.FLOAT, "l2"), knn(8, VectorDataType.BYTE, "l2"));
    assertEquals(VectorDataType.FLOAT, floatAndByte.getDataType());
    assertEquals(VectorFieldType.UNKNOWN_DIMENSION, floatAndByte.getDimension());
    assertTrue(floatAndByte.hasMappingMismatch());
    OpenSearchKnnVectorType byteAndBinary =
        mergeVectors(knn(64, VectorDataType.BYTE, "l2"), knn(64, VectorDataType.BINARY, "hamming"));
    assertEquals(VectorDataType.BYTE, byteAndBinary.getDataType());
    assertEquals(VectorFieldType.UNKNOWN_DIMENSION, byteAndBinary.getDimension());
    assertNull(byteAndBinary.getSpaceType());
    assertTrue(byteAndBinary.hasMappingMismatch());
  }

  /** float and half_float read their elements the same way, so the dimension is kept. */
  @Test
  void floatAndHalfFloatKeepTheDimensionButMismatch() {
    OpenSearchKnnVectorType merged =
        mergeVectors(knn(8, VectorDataType.FLOAT, "l2"), knn(8, VectorDataType.HALF_FLOAT, "l2"));
    assertEquals(8, merged.getDimension());
    assertEquals(VectorDataType.FLOAT, merged.getDataType());
    assertTrue(merged.hasMappingMismatch());
    assertEquals(VectorDataType.FLOAT, merged.getStoredDataType(null));
  }

  /** Only the indices' agreement on element bytes decides how a base64 value is decoded. */
  @Test
  void storedDataTypeIsTheIndexOwnWhenElementEncodingsDiffer() {
    OpenSearchKnnVectorType merged =
        mergeVectors(knn(4, VectorDataType.FLOAT, "l2"), knn(4, VectorDataType.BYTE, "l2"));
    assertAll(
        () -> assertEquals(VectorDataType.FLOAT, merged.getStoredDataType("i0")),
        () -> assertEquals(VectorDataType.BYTE, merged.getStoredDataType("i1")),
        () -> assertNull(merged.getStoredDataType(null)),
        () -> assertNull(merged.getStoredDataType("other")));
    OpenSearchKnnVectorType byteAndBinary =
        mergeVectors(knn(64, VectorDataType.BYTE, "l2"), knn(64, VectorDataType.BINARY, "hamming"));
    assertEquals(VectorDataType.BYTE, byteAndBinary.getStoredDataType(null));
  }

  /** As when knn_vector mapped to ARRAY: deep-merged with a nested field, first index kept. */
  @Test
  void vectorAndNestedKeepTheFirstIndexType() {
    assertInstanceOf(
        OpenSearchKnnVectorType.class,
        merge(knn(8, VectorDataType.FLOAT, "l2"), OpenSearchDataType.of(MappingType.Nested)));
    assertEquals(
        MappingType.Nested,
        merge(OpenSearchDataType.of(MappingType.Nested), knn(8, VectorDataType.FLOAT, "l2"))
            .getMappingType());
  }

  /**
   * The sub-fields a nested field merges into a vector field are kept through a later vector merge,
   * in either order, and merging them does not fail.
   */
  @Test
  void vectorMergeKeepsTheSubFieldsOfANestedField() {
    OpenSearchDataType nested =
        OpenSearchDataType.of(
            MappingType.Nested, Map.of("properties", Map.of("sub", Map.of("type", "integer"))));
    for (OpenSearchDataType merged :
        List.of(
            merge(
                knn(768, VectorDataType.FLOAT, "l2"), knn(384, VectorDataType.FLOAT, "l2"), nested),
            merge(
                knn(768, VectorDataType.FLOAT, "l2"), nested, knn(384, VectorDataType.FLOAT, "l2")),
            merge(
                knn(768, VectorDataType.FLOAT, "l2"),
                knn(768, VectorDataType.BYTE, "l2"),
                nested))) {
      assertInstanceOf(OpenSearchKnnVectorType.class, merged);
      assertTrue(((OpenSearchKnnVectorType) merged).hasMappingMismatch());
      assertEquals(MappingType.Integer, merged.getProperties().get("sub").getMappingType());
    }
  }

  /** As before: any other conflict keeps the last index's type. */
  @Test
  void vectorAndScalarKeepTheLastIndexType() {
    assertEquals(
        MappingType.Float,
        merge(knn(8, VectorDataType.FLOAT, "l2"), OpenSearchDataType.of(MappingType.Float))
            .getMappingType());
    assertInstanceOf(
        OpenSearchKnnVectorType.class,
        merge(OpenSearchDataType.of(MappingType.Float), knn(8, VectorDataType.FLOAT, "l2")));
  }

  @Test
  void flatteningKeepsTheMismatch() {
    OpenSearchDataType merged =
        merge(knn(768, VectorDataType.FLOAT, "l2"), knn(384, VectorDataType.FLOAT, "l2"));
    OpenSearchKnnVectorType flattened =
        (OpenSearchKnnVectorType)
            OpenSearchDataType.traverseAndFlatten(Map.of("f", merged)).get("f");
    assertTrue(flattened.hasMappingMismatch());
    assertEquals(List.of("i0", "i1"), flattened.getMappingMismatchIndices());
    assertEquals(merged, flattened);
  }
}
