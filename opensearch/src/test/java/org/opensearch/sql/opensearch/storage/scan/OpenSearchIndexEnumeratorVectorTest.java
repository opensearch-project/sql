/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.monitor.AlwaysHealthyMonitor.ALWAYS_HEALTHY_MONITOR;
import static org.opensearch.sql.opensearch.response.AggregationResponseUtils.fromJson;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.data.type.VectorFieldType;
import org.opensearch.sql.data.utils.ComparableLinkedHashMap;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprEncodedVectorValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.response.ShardStats;
import org.opensearch.sql.opensearch.response.agg.TopHitsParser;

/**
 * The Calcite scan converts vectors, read as stored, to the element type of their column, also
 * inside object and nested columns. Other values are as {@code valueForCalcite()} gives them.
 */
class OpenSearchIndexEnumeratorVectorTest {

  private static final Map<String, VectorFieldType> VECTOR_FIELDS =
      Map.of(
          "f", OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, "l2", null, null),
          "b", OpenSearchKnnVectorType.of(2, VectorDataType.BYTE, null, null, null),
          "obj.vec", OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, null, null, null),
          "nest.vec", OpenSearchKnnVectorType.of(2, VectorDataType.FLOAT, null, null, null));

  private static OpenSearchIndexEnumerator enumerator(
      ExprValue row, List<String> fields, List<RelDataType> types) {
    return enumerator(row, fields, types, VECTOR_FIELDS);
  }

  private static OpenSearchIndexEnumerator enumerator(
      ExprValue row,
      List<String> fields,
      List<RelDataType> types,
      Map<String, VectorFieldType> vectorFields) {
    OpenSearchClient client = mock(OpenSearchClient.class);
    OpenSearchRequest request = mock(OpenSearchRequest.class);
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    when(client.search(request)).thenReturn(response);
    when(response.isEmpty()).thenReturn(false);
    when(response.getShardStats()).thenReturn(ShardStats.UNKNOWN);
    when(response.getHitsSize()).thenReturn(1);
    when(response.iterator()).thenReturn(List.of(row).iterator());
    OpenSearchIndexEnumerator enumerator =
        new OpenSearchIndexEnumerator(
            client, fields, types, vectorFields, 100, 10, 10, request, ALWAYS_HEALTHY_MONITOR);
    assertTrue(enumerator.moveNext());
    return enumerator;
  }

  private static RelDataType anyType() {
    return TYPE_FACTORY.createSqlType(SqlTypeName.ANY);
  }

  /** The type of an object column. */
  private static RelDataType objectType() {
    return TYPE_FACTORY.createMapType(
        TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), anyType(), true);
  }

  /** The type of a nested column. */
  private static RelDataType nestedType() {
    return TYPE_FACTORY.createArrayType(anyType(), -1);
  }

  private static ExprValue tuple(Object... keysAndValues) {
    Map<String, ExprValue> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      Object value = keysAndValues[i + 1];
      map.put(
          (String) keysAndValues[i],
          value instanceof ExprValue exprValue ? exprValue : ExprValueUtils.fromObjectValue(value));
    }
    return ExprTupleValue.fromExprValueMap(map);
  }

  @Test
  void vectorColumnsGetFloatAndByteElements() {
    ExprValue row =
        tuple(
            "id", 1,
            "f", Arrays.asList(1, 2.5d, -0.006929283495992422d),
            "b", Arrays.asList(-128, 127));
    assertArrayEquals(
        new Object[] {
          1, List.of(1.0f, 2.5f, -0.006929283495992422f), List.of((byte) -128, (byte) 127)
        },
        (Object[])
            enumerator(
                    row,
                    List.of("id", "f", "b"),
                    List.of(
                        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                        TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, "l2", true),
                        TYPE_FACTORY.createVectorType(2, VectorDataType.BYTE, null, true)))
                .current());
  }

  /** k-NN accepts a base64 string (little-endian float32 for float vectors) and a number. */
  @Test
  void encodedVectorsAreDecoded() {
    // 1.0f, -2.5f little-endian
    String base64 = "AACAPwAAIMA=";
    ExprValue row =
        tuple(
            "f",
            new OpenSearchExprEncodedVectorValue(base64),
            "b",
            new OpenSearchExprEncodedVectorValue("Af8="));
    assertArrayEquals(
        new Object[] {List.of(1.0f, -2.5f), List.of((byte) 1, (byte) -1)},
        (Object[])
            enumerator(
                    row,
                    List.of("f", "b"),
                    List.of(
                        TYPE_FACTORY.createVectorType(2, VectorDataType.FLOAT, "l2", true),
                        TYPE_FACTORY.createVectorType(2, VectorDataType.BYTE, null, true)))
                .current());
    assertEquals(
        List.of(7.0f),
        enumerator(
                tuple("f", new OpenSearchExprEncodedVectorValue(7.0d)),
                List.of("f"),
                List.of(TYPE_FACTORY.createVectorType(1, VectorDataType.FLOAT, null, true)))
            .current());
  }

  @Test
  void vectorsInsideObjectColumnsAreConverted() {
    ExprValue row = tuple("obj", tuple("vec", Arrays.asList(1, 2, 3), "name", "x"));
    Object obj = enumerator(row, List.of("obj"), List.of(objectType())).current();
    Map<String, Object> expected = new ComparableLinkedHashMap<>();
    expected.put("vec", List.of(1.0f, 2.0f, 3.0f));
    expected.put("name", "x");
    assertEquals(expected, obj);
    assertTrue(obj instanceof ComparableLinkedHashMap);
    // The flattened column of the same field.
    assertEquals(
        List.of(1.0f, 2.0f, 3.0f),
        enumerator(
                row,
                List.of("obj.vec"),
                List.of(TYPE_FACTORY.createVectorType(3, VectorDataType.FLOAT, null, true)))
            .current());
  }

  @Test
  void vectorsInsideNestedColumnsAreConverted() {
    ExprValue row =
        tuple(
            "nest",
            new ExprCollectionValue(
                List.of(
                    tuple("vec", Arrays.asList(1, 2), "n", 1),
                    tuple("vec", Arrays.asList(3.5d, 4), "n", 2))));
    assertEquals(
        List.of(
            Map.of("vec", List.of(1.0f, 2.0f), "n", 1), Map.of("vec", List.of(3.5f, 4.0f), "n", 2)),
        enumerator(row, List.of("nest"), List.of(nestedType())).current());
  }

  @Test
  void otherColumnsAreUnchanged() {
    ExprValue row = tuple("other", tuple("vec", Arrays.asList(1, 2, 3)), "list", List.of(1, 2));
    Object[] values =
        (Object[])
            enumerator(row, List.of("other", "list"), List.of(objectType(), nestedType()))
                .current();
    assertEquals(row.tupleValue().get("other").valueForCalcite(), values[0]);
    assertEquals(List.of(1, 2), values[1]);
  }

  /**
   * A column is converted by its type, not by its name: an aggregation output can be named like a
   * vector field or an object holding one, e.g. {@code stats count() as f}.
   */
  @Test
  void columnsNamedLikeVectorFieldsAreConvertedByTheirType() {
    RelDataType bigint = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    Object[] values =
        (Object[])
            enumerator(
                    tuple("f", 3L, "obj", 4L, "obj.vec", 5L),
                    List.of("f", "obj", "obj.vec"),
                    List.of(bigint, bigint, bigint))
                .current();
    assertArrayEquals(new Object[] {3L, 4L, 5L}, values);
    // An ARRAY column named like a nested field holding a vector, e.g. the output of values().
    assertEquals(
        List.of("a", "b"),
        enumerator(
                tuple("nest", new ExprCollectionValue(List.of(stringValue("a"), stringValue("b")))),
                List.of("nest"),
                List.of(nestedType()))
            .current());
  }

  /** A field that a float index and a byte index map: index i0 with float, i1 with byte. */
  private static OpenSearchKnnVectorType mixedEncodingField() {
    return OpenSearchKnnVectorType.of(4, VectorDataType.FLOAT, "l2", null, null)
        .inIndex("i0")
        .mergedWith(
            OpenSearchKnnVectorType.of(4, VectorDataType.BYTE, "l2", null, null).inIndex("i1"));
  }

  /** The row of a hit of the given index, as the response reads it for a merged mapping. */
  private static ExprValue hit(
      OpenSearchKnnVectorType field, String source, @Nullable String index) {
    OpenSearchExprValueFactory factory =
        new OpenSearchExprValueFactory(Map.of("f", field, "obj", objectWith(field)), true);
    ExprValue row = factory.construct(source, false);
    if (index != null) {
      factory.recordHitIndex(row, index);
    }
    return row;
  }

  private static OpenSearchDataType objectWith(OpenSearchKnnVectorType field) {
    OpenSearchDataType obj =
        OpenSearchDataType.of(
            OpenSearchDataType.MappingType.Object, Map.of("properties", Map.of()));
    obj.getProperties().put("vec", field);
    return obj;
  }

  /**
   * A base64 vector is decoded with the data type of its own index when the indices disagree on
   * whether elements are bytes: four bytes from the byte index, one float32 from the float index.
   */
  @Test
  void base64OfMixedEncodingFieldIsDecodedWithItsIndexDataType() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    assertEquals(VectorDataType.FLOAT, field.getDataType());
    Map<String, VectorFieldType> vectorFields = Map.of("f", field, "obj.vec", field);
    RelDataType column = TYPE_FACTORY.createVectorType(field, true);
    // "AQID/w==" is the bytes 1, 2, 3, -1; read as one float32 it would be about -1.7e38.
    String source = "{\"f\": \"AQID/w==\", \"obj\": {\"vec\": \"AQID/w==\"}}";
    Object[] fromByteIndex =
        (Object[])
            enumerator(
                    hit(field, source, "i1"),
                    List.of("f", "obj"),
                    List.of(column, objectType()),
                    vectorFields)
                .current();
    assertEquals(List.of(1.0f, 2.0f, 3.0f, -1.0f), fromByteIndex[0]);
    assertEquals(List.of(1.0f, 2.0f, 3.0f, -1.0f), ((Map<?, ?>) fromByteIndex[1]).get("vec"));
    // "AACAPw==" is 1.0f little-endian.
    assertEquals(
        List.of(1.0f),
        enumerator(
                hit(field, "{\"f\": \"AACAPw==\"}", "i0"),
                List.of("f"),
                List.of(column),
                vectorFields)
            .current());
    // Arrays of numbers do not depend on the index.
    assertEquals(
        List.of(1.0f, 2.0f),
        enumerator(hit(field, "{\"f\": [1, 2]}", null), List.of("f"), List.of(column), vectorFields)
            .current());
  }

  /** Without the index of the value, a base64 vector of such a field fails rather than misread. */
  @Test
  void base64OfMixedEncodingFieldWithoutItsIndexFails() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    OpenSearchIndexEnumerator enumerator =
        enumerator(
            hit(field, "{\"f\": \"AQID/w==\"}", null),
            List.of("f"),
            List.of(TYPE_FACTORY.createVectorType(field, true)),
            Map.of("f", field));
    SemanticCheckException e = assertThrows(SemanticCheckException.class, enumerator::current);
    assertEquals(
        "cannot decode the base64 vector of [f]: the indices of the query map it with float and"
            + " byte data types, and the index of this value is not known",
        e.getMessage());
  }

  /**
   * The rows of an aggregation response, as {@code OpenSearchResponse} builds them from what a
   * parser returns: each entry read by name with the value factory of the index's mapping, extended
   * with the output columns' types as {@code CalciteLogicalIndexScan} extends it.
   */
  private static List<ExprValue> aggregationRows(
      Map<String, OpenSearchDataType> mapping,
      Map<String, RelDataType> outputTypes,
      TopHitsParser parser,
      String response) {
    OpenSearchExprValueFactory factory = new OpenSearchExprValueFactory(mapping, true);
    Map<String, OpenSearchDataType> extended = new LinkedHashMap<>();
    outputTypes.forEach(
        (name, type) ->
            extended.put(
                name,
                OpenSearchDataType.of(OpenSearchTypeFactory.convertRelDataTypeToExprType(type))));
    factory.extendTypeMapping(extended);
    return parser.parse(fromJson(response).get(parser.getName())).stream()
        .map(
            entry -> {
              Map<String, ExprValue> row = new LinkedHashMap<>();
              entry.forEach((key, value) -> row.put(key, factory.construct(key, value, true)));
              return (ExprValue) ExprTupleValue.fromExprValueMap(row);
            })
        .toList();
  }

  private static String topHits(String name, String hits) {
    return "{\"top_hits#"
        + name
        + "\":{\"hits\":{\"total\":{\"value\":2,\"relation\":\"eq\"},\"max_score\":1.0,"
        + "\"hits\":["
        + hits
        + "]}}}";
  }

  /**
   * {@code stats first(f)} with pushdown: the top_hits reads the vector from {@code _source} and
   * the column gets the whole vector, as without pushdown.
   */
  @Test
  void firstOfAVectorFromATopHitsResponseIsTheWholeVector() {
    OpenSearchKnnVectorType field =
        OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, null, null, null);
    List<ExprValue> rows =
        aggregationRows(
            Map.of("f", field),
            Map.of("first(f)", TYPE_FACTORY.createVectorType(field, true)),
            new TopHitsParser("first(f)", true, false),
            topHits(
                "first(f)",
                "{\"_index\":\"i0\",\"_id\":\"1\",\"_score\":1.0,"
                    + "\"_source\":{\"f\":[1,2.5,3]}}"));
    assertEquals(
        List.of(1.0f, 2.5f, 3.0f),
        enumerator(
                rows.getFirst(),
                List.of("first(f)"),
                List.of(TYPE_FACTORY.createVectorType(field, true)),
                Map.of("f", field))
            .current());
  }

  /**
   * A top_hits over a field whose indices disagree on element encoding records the hit's index, so
   * an aggregation output, which no field is named after, decodes base64 with that index's data
   * type; without it, it fails.
   */
  @Test
  void firstOfAMixedEncodingVectorDecodesWithTheHitIndex() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    RelDataType column = TYPE_FACTORY.createVectorType(field, true);
    String hit =
        "{\"_index\":\"i1\",\"_id\":\"1\",\"_score\":1.0,\"_source\":{\"f\":\"AQID/w==\"}}";
    List<ExprValue> rows =
        aggregationRows(
            Map.of("f", field),
            Map.of("first(f)", column),
            new TopHitsParser("first(f)", true, false, null, true),
            topHits("first(f)", hit));
    assertEquals(
        List.of(1.0f, 2.0f, 3.0f, -1.0f),
        enumerator(rows.getFirst(), List.of("first(f)"), List.of(column), Map.of("f", field))
            .current());
    List<ExprValue> withoutIndex =
        aggregationRows(
            Map.of("f", field),
            Map.of("first(f)", column),
            new TopHitsParser("first(f)", true, false),
            topHits("first(f)", hit));
    OpenSearchIndexEnumerator enumerator =
        enumerator(
            withoutIndex.getFirst(), List.of("first(f)"), List.of(column), Map.of("f", field));
    SemanticCheckException e = assertThrows(SemanticCheckException.class, enumerator::current);
    assertEquals(
        "cannot decode the base64 vector of [first(f)]: the indices of the query map it with float"
            + " and byte data types, and the index of this value is not known",
        e.getMessage());
  }

  /** A dedup row records its hit's index, also for a renamed column and a vector in an object. */
  @Test
  void dedupRowOfAMixedEncodingVectorDecodesWithTheHitIndex() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    List<ExprValue> rows =
        aggregationRows(
            Map.of("f", field, "obj", objectWith(field)),
            Map.of("w", TYPE_FACTORY.createVectorType(field, true)),
            new TopHitsParser("dedup", false, false, Map.of("f", List.of("w")), true),
            topHits(
                "dedup",
                "{\"_index\":\"i1\",\"_id\":\"1\",\"_score\":1.0,\"_source\":"
                    + "{\"f\":\"AQID/w==\",\"obj\":{\"vec\":\"AQID/w==\"}}}"));
    Object[] values =
        (Object[])
            enumerator(
                    rows.getFirst(),
                    List.of("w", "obj"),
                    List.of(TYPE_FACTORY.createVectorType(field, true), objectType()),
                    Map.of("f", field, "obj.vec", field))
                .current();
    assertEquals(List.of(1.0f, 2.0f, 3.0f, -1.0f), values[0]);
    assertEquals(List.of(1.0f, 2.0f, 3.0f, -1.0f), ((Map<?, ?>) values[1]).get("vec"));
  }

  /** An index outside the merge, e.g. one created after the mapping was read, is named. */
  @Test
  void base64FromAnIndexOutsideTheMergeFails() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    OpenSearchIndexEnumerator enumerator =
        enumerator(
            hit(field, "{\"f\": \"AQID/w==\"}", "i9"),
            List.of("f"),
            List.of(TYPE_FACTORY.createVectorType(field, true)),
            Map.of("f", field));
    SemanticCheckException e = assertThrows(SemanticCheckException.class, enumerator::current);
    assertEquals(
        "cannot decode the base64 vector of [f] from index [i9]: the indices of the query map it"
            + " with float and byte data types, and the mapping of index [i9] is not known",
        e.getMessage());
  }

  /** A single number does not depend on the element encoding, so it needs no index. */
  @Test
  void singleNumberOfAMixedEncodingFieldNeedsNoIndex() {
    OpenSearchKnnVectorType field = mixedEncodingField();
    assertEquals(
        List.of(7.0f),
        enumerator(
                hit(field, "{\"f\": 7}", null),
                List.of("f"),
                List.of(TYPE_FACTORY.createVectorType(field, true)),
                Map.of("f", field))
            .current());
  }
}
