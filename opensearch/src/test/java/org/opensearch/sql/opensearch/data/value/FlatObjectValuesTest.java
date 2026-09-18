/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprVariantTupleValue;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;

class FlatObjectValuesTest {

  @Test
  void flatten_leavesKeepTheirTypeAndNullIsKept() {
    Map<String, Object> source = new HashMap<>();
    source.put("n", 12.5);
    source.put("i", 503);
    source.put("s", "4");
    source.put("z", null);
    ExprValue value = FlatObjectValues.flatten(new ObjectContent(source));
    assertAll(
        () -> assertInstanceOf(ExprVariantTupleValue.class, value),
        () -> assertEquals(doubleValue(12.5), value.tupleValue().get("n")),
        () -> assertEquals(integerValue(503), value.tupleValue().get("i")),
        () -> assertEquals(stringValue("4"), value.tupleValue().get("s")),
        () -> assertEquals(nullValue(), value.tupleValue().get("z")));
  }

  @Test
  void flatten_nonObjectIsNullValue() {
    assertEquals(nullValue(), FlatObjectValues.flatten(new ObjectContent(5)));
  }

  // An array leaf stays an array; each element is typed on its own, so a number next to a string
  // is a number next to a string, not two strings.
  @Test
  void flatten_arrayStaysAnArrayWithTypedElements() {
    Map<String, Object> source =
        Map.of(
            "tags", List.of("a", "b"), "ports", List.of(80, 443), "mixed", List.of(1, "x", true));
    ExprValue value = FlatObjectValues.flatten(new ObjectContent(source));
    assertAll(
        () -> assertEquals(collectionValue(List.of("a", "b")), value.tupleValue().get("tags")),
        () -> assertEquals(collectionValue(List.of(80, 443)), value.tupleValue().get("ports")),
        () ->
            assertEquals(
                new ExprCollectionValue(
                    List.of(integerValue(1), stringValue("x"), booleanValue(true))),
                value.tupleValue().get("mixed")));
  }

  // The index files one term per element under the same dotted path -- attributes.nested.k holds
  // both 1 and 2 -- so the elements fold into that path rather than staying an array of objects.
  @Test
  void flatten_objectInsideArrayFoldsIntoTheDottedPath() {
    Map<String, Object> source = Map.of("nested", List.of(Map.of("k", 1), Map.of("k", 2)));
    ExprValue value = FlatObjectValues.flatten(new ObjectContent(source));
    assertAll(
        () ->
            assertEquals(
                new ExprCollectionValue(List.of(integerValue(1), integerValue(2))),
                value.tupleValue().get("nested.k")),
        () -> assertNull(value.tupleValue().get("nested")));
  }

  // A path written twice in one document holds both values, as the index does. Both spellings of a
  // dotted path collide on the one key, and neither value may be dropped.
  @Test
  void flatten_aPathWrittenTwiceHoldsBothValues() {
    LinkedHashMap<String, Object> nestedFirst = new LinkedHashMap<>();
    nestedFirst.put("a", Map.of("b", 1));
    nestedFirst.put("a.b", 2);
    LinkedHashMap<String, Object> dottedFirst = new LinkedHashMap<>();
    dottedFirst.put("a.b", 2);
    dottedFirst.put("a", Map.of("b", 1));
    assertAll(
        () ->
            assertEquals(
                List.of(integerValue(1), integerValue(2)),
                sorted(FlatObjectValues.flatten(new ObjectContent(nestedFirst)), "a.b")),
        () ->
            assertEquals(
                List.of(integerValue(1), integerValue(2)),
                sorted(FlatObjectValues.flatten(new ObjectContent(dottedFirst)), "a.b")));
  }

  // OpenSearch accepts an array at the top level of a flat_object and folds every element into the
  // same terms; this used to reach parseArray instead and leave the elements unflattened.
  @Test
  void flatten_topLevelArrayFoldsItsElements() {
    ExprValue one =
        FlatObjectValues.flatten(new ObjectContent(List.of(Map.of("a", Map.of("b", 1)))));
    ExprValue two =
        FlatObjectValues.flatten(new ObjectContent(List.of(Map.of("a", 1), Map.of("a", 2))));
    assertAll(
        () -> assertEquals(integerValue(1), one.tupleValue().get("a.b")),
        () ->
            assertEquals(
                new ExprCollectionValue(List.of(integerValue(1), integerValue(2))),
                two.tupleValue().get("a")));
  }

  @Test
  void flatten_scalarIsNullValue() {
    assertEquals(nullValue(), FlatObjectValues.flatten(new ObjectContent(5)));
  }

  private static List<ExprValue> sorted(ExprValue flattened, String key) {
    List<ExprValue> values = new ArrayList<>(flattened.tupleValue().get(key).collectionValue());
    values.sort((a, b) -> Integer.compare(a.integerValue(), b.integerValue()));
    return values;
  }
}
