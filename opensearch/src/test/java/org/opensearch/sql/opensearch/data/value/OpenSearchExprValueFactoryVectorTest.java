/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.VectorDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;

/**
 * A knn_vector value is parsed exactly as it was when the field mapped to ARRAY: each element keeps
 * the type its JSON has. SQL and the V2 engine return these values; the Calcite scan converts them
 * to Float or Byte elements (see {@code OpenSearchIndexEnumerator}).
 */
class OpenSearchExprValueFactoryVectorTest {

  private static final Map<String, OpenSearchDataType> MAPPING =
      Map.of(
          "f",
          OpenSearchKnnVectorType.of(3, VectorDataType.FLOAT, "l2", "lucene", "hnsw"),
          "b",
          OpenSearchKnnVectorType.of(3, VectorDataType.BYTE, null, null, null),
          "plain",
          OpenSearchDataType.of(ExprCoreType.VECTOR),
          "nested",
          OpenSearchDataType.of(OpenSearchDataType.MappingType.Nested),
          "name",
          OpenSearchDataType.of(OpenSearchDataType.MappingType.Keyword));

  private static final OpenSearchExprValueFactory FACTORY =
      new OpenSearchExprValueFactory(MAPPING, true);
  private static final OpenSearchExprValueFactory STRICT_FACTORY =
      new OpenSearchExprValueFactory(MAPPING, false);

  private static Map<String, ExprValue> source(OpenSearchExprValueFactory factory, String json) {
    return factory.construct(json, false).tupleValue();
  }

  @Test
  void elementsKeepTheirJsonTypes() {
    ExprValue value = source(FACTORY, "{\"f\": [1, 2.5, -0.006929283495992422]}").get("f");
    assertEquals(List.of(1, 2.5, -0.006929283495992422), value.valueForCalcite());
  }

  @Test
  void byteVectorElementsKeepTheirJsonTypes() {
    assertEquals(
        List.of(-128, 0, 127),
        source(FACTORY, "{\"b\": [-128, 0, 127]}").get("b").valueForCalcite());
  }

  /** The same JSON parses to the same value as it did for the ARRAY type knn_vector mapped to. */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "[1, 2, 3]",
        "[0.5, -0.006929283495992422, 1e-7]",
        "[1, \"x\", 3]",
        "[]",
        "null",
        "7",
        "[[1, 2], [3, 4]]",
        "\"AACAPwAAIMA=\"",
        "{\"a\": 1}"
      })
  void parsesLikeTheArrayTypeOfBefore(String json) {
    for (OpenSearchExprValueFactory factory : List.of(FACTORY, STRICT_FACTORY)) {
      Map<String, ExprValue> row =
          source(
              factory, "{\"f\": " + json + ", \"plain\": " + json + ", \"nested\": " + json + "}");
      assertEquals(row.get("nested"), row.get("f"), json);
      assertEquals(row.get("nested"), row.get("plain"), json);
    }
  }

  /**
   * A base64 string or a single number reads as the empty array it read as before, and keeps the
   * stored value for the Calcite scan to decode.
   */
  @Test
  void encodedVectorsKeepTheStoredValueForCalcite() {
    ExprValue base64 = source(FACTORY, "{\"f\": \"AACAPwAAIMA=\"}").get("f");
    assertTrue(base64 instanceof OpenSearchExprEncodedVectorValue);
    assertEquals(List.of(), base64.value());
    assertEquals(List.of(), base64.collectionValue());
    assertEquals("[]", base64.toString());
    assertEquals("AACAPwAAIMA=", base64.valueForCalcite());
    ExprValue number = source(FACTORY, "{\"f\": 7}").get("f");
    assertEquals(List.of(), number.value());
    assertEquals(7.0d, number.valueForCalcite());
  }

  /**
   * The map content of an aggregation response (top_hits of dedup, first, last, take) reads a
   * base64 string or a single number as the hit path does: an empty array for SQL and V2, the
   * stored value for Calcite. It used to fail with a ClassCastException.
   */
  @Test
  void encodedVectorsFromAggregationResponses() {
    ExprValue base64 = FACTORY.construct("f", "AACAPwAAIMA=", true);
    assertTrue(base64 instanceof OpenSearchExprEncodedVectorValue);
    assertEquals(List.of(), base64.value());
    assertEquals("AACAPwAAIMA=", base64.valueForCalcite());
    ExprValue number = FACTORY.construct("f", 7, true);
    assertEquals(List.of(), number.value());
    assertEquals(7.0d, number.valueForCalcite());
    // An array from a map reads as before.
    assertEquals(List.of(1, 2.5d), FACTORY.construct("f", List.of(1, 2.5d), true).value());
  }

  /**
   * For a field whose indices disagree on whether elements are bytes, the hit's index is recorded
   * on its base64 values, also inside objects and nested arrays, so that the scan decodes each with
   * its own index's data type; other fields do not need it.
   */
  @Test
  void indexOfTheHitIsRecordedOnMixedEncodingVectors() {
    OpenSearchKnnVectorType mixed =
        OpenSearchKnnVectorType.of(4, VectorDataType.FLOAT, null, null, null)
            .inIndex("i0")
            .mergedWith(
                OpenSearchKnnVectorType.of(4, VectorDataType.BYTE, null, null, null).inIndex("i1"));
    OpenSearchDataType nested =
        OpenSearchDataType.of(
            OpenSearchDataType.MappingType.Nested, Map.of("properties", Map.of()));
    nested.getProperties().put("vec", mixed);
    OpenSearchExprValueFactory factory =
        new OpenSearchExprValueFactory(
            Map.of("m", mixed, "f", MAPPING.get("f"), "nest", nested), true);
    ExprValue source =
        factory.construct(
            "{\"m\": \"AQID/w==\", \"f\": \"AACAPw==\","
                + " \"nest\": [{\"vec\": \"AQID/w==\"}, {\"vec\": [1, 2, 3, 4]}]}",
            true);
    factory.recordHitIndex(source, "i1");
    Map<String, ExprValue> row = source.tupleValue();
    assertEquals("i1", ((OpenSearchExprEncodedVectorValue) row.get("m")).getIndex());
    assertNull(((OpenSearchExprEncodedVectorValue) row.get("f")).getIndex());
    ExprValue firstNested = row.get("nest").collectionValue().getFirst();
    assertEquals(
        "i1", ((OpenSearchExprEncodedVectorValue) firstNested.tupleValue().get("vec")).getIndex());
    // Until the index is recorded, there is none.
    assertNull(
        ((OpenSearchExprEncodedVectorValue)
                factory.construct("{\"m\": \"AQID/w==\"}", false).tupleValue().get("m"))
            .getIndex());
  }

  @Test
  void wholeArrayIsKeptWithoutArraySupport() {
    // Without field type tolerance, an array in a scalar field keeps only its first element. A
    // vector is a single value, so it is kept whole.
    assertEquals(
        List.of(1, 2, 3), source(STRICT_FACTORY, "{\"f\": [1, 2, 3]}").get("f").valueForCalcite());
  }

  @Test
  void fieldExcludedFromSourceIsAbsent() {
    Map<String, ExprValue> row = source(FACTORY, "{\"name\": \"a\"}");
    assertFalse(row.containsKey("f"));
  }

  @Test
  void nullAndEmptyValuesReadAsNull() {
    assertTrue(source(FACTORY, "{\"f\": null}").get("f").isNull());
    assertTrue(source(FACTORY, "{\"f\": []}").get("f").isNull());
  }

  @Test
  void constructFromObjectValue() {
    ExprValue value = FACTORY.construct("f", List.of(1.0d, 2, 3.5d), true);
    assertEquals(List.of(1.0d, 2, 3.5d), value.valueForCalcite());
  }
}
