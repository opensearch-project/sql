/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.utils.ObjectContent;

class FlatObjectValuesTest {

  private static ExprValue flatten(Object source) {
    return FlatObjectValues.flatten(new ObjectContent(source));
  }

  // A leaf has no type of its own in the index, so every value reads as its text -- the same rule
  // spath applies to a JSON string.
  @Test
  void everyLeafReadsAsText() {
    Map<String, Object> source = new LinkedHashMap<>();
    source.put("n", 12.5);
    source.put("i", 503);
    source.put("s", "4");
    source.put("b", true);
    source.put("z", null);
    ExprValue value = flatten(source);
    assertAll(
        () -> assertEquals(stringValue("12.5"), value.tupleValue().get("n")),
        () -> assertEquals(stringValue("503"), value.tupleValue().get("i")),
        () -> assertEquals(stringValue("4"), value.tupleValue().get("s")),
        () -> assertEquals(stringValue("true"), value.tupleValue().get("b")),
        () -> assertEquals(nullValue(), value.tupleValue().get("z")));
  }

  // Both spellings of a path reach the same leaf, which is what the index holds: one term with the
  // path folded in, whichever way the document wrote it.
  @Test
  void bothSpellingsOfAPathReachTheSameLeaf() {
    assertAll(
        () ->
            assertEquals(
                stringValue("1"), flatten(Map.of("a", Map.of("b", 1))).tupleValue().get("a.b")),
        () -> assertEquals(stringValue("1"), flatten(Map.of("a.b", 1)).tupleValue().get("a.b")));
  }

  // The index files a term per value, so a path that receives several holds them all, as the JSON
  // of those values -- again what spath returns for an array inside a JSON string.
  @Test
  void aPathWithSeveralValuesReadsAsJson() {
    LinkedHashMap<String, Object> collide = new LinkedHashMap<>();
    collide.put("a", Map.of("b", 1));
    collide.put("a.b", 2);
    LinkedHashMap<String, Object> collideOtherOrder = new LinkedHashMap<>();
    collideOtherOrder.put("a.b", 2);
    collideOtherOrder.put("a", Map.of("b", 1));
    assertAll(
        // an array leaf
        () ->
            assertEquals(
                stringValue("[\"a\",\"b\"]"),
                flatten(Map.of("tags", List.of("a", "b"))).tupleValue().get("tags")),
        // elements of different types still read as text
        () ->
            assertEquals(
                stringValue("[\"1\",\"x\"]"),
                flatten(Map.of("mixed", List.of(1, "x"))).tupleValue().get("mixed")),
        // an object inside an array folds into the dotted path, one term per element
        () ->
            assertEquals(
                stringValue("[\"1\",\"2\"]"),
                flatten(Map.of("nested", List.of(Map.of("k", 1), Map.of("k", 2))))
                    .tupleValue()
                    .get("nested.k")),
        // an array at the top level, which OpenSearch also accepts
        () ->
            assertEquals(
                stringValue("[\"1\",\"2\"]"),
                flatten(List.of(Map.of("p", 1), Map.of("p", 2))).tupleValue().get("p")),
        // both spellings present in one document: neither value may be dropped. The order follows
        // the document, as _source order does, so the two spellings read in the order written
        () -> assertEquals(stringValue("[\"1\",\"2\"]"), flatten(collide).tupleValue().get("a.b")),
        () ->
            assertEquals(
                stringValue("[\"2\",\"1\"]"), flatten(collideOtherOrder).tupleValue().get("a.b")));
  }

  @Test
  void aScalarWhereAnObjectWasPromisedIsNull() {
    assertEquals(nullValue(), flatten(5));
  }

  // A flat_object exists so documents can escape the mapping depth limit, so nothing upstream
  // bounds the recursion: levels past the cap are kept as one leaf rather than dropped.
  @Test
  void depthIsCapped() {
    Map<String, Object> deepest = Map.of("leaf", "v");
    Map<String, Object> source = deepest;
    for (int i = 0; i < 25; i++) {
      source = Map.of("l" + i, source);
    }
    ExprValue value = flatten(source);
    assertEquals(1, value.tupleValue().size());
    String key = value.tupleValue().keySet().iterator().next();
    // MAX_DEPTH levels are descended, so the key is that many prefixes plus the leaf's own name
    assertEquals(FlatObjectValues.MAX_DEPTH + 1, key.split("\\.").length);
  }
}
