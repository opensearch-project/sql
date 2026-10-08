/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

class ObjectScalarConflictRuleTest {

  private final ObjectScalarConflictRule rule = new ObjectScalarConflictRule();

  /** An object type mapping the given keyword sub-fields. */
  private static OpenSearchDataType object(String... subFields) {
    Map<String, Object> raw = new LinkedHashMap<>();
    for (String subField : subFields) {
      raw.put(subField, Map.of("type", "keyword"));
    }
    return OpenSearchDataType.of(MappingType.Object, Map.of("properties", raw));
  }

  @Test
  void matchesObjectAgainstScalarInBothDirections() {
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertTrue(rule.isMatch(object("name"), keyword));
    assertTrue(rule.isMatch(keyword, object("name")));
  }

  @Test
  void matchesNestedAgainstScalar() {
    OpenSearchDataType nested = OpenSearchDataType.of(MappingType.Nested);
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertTrue(rule.isMatch(nested, keyword));
    assertTrue(rule.isMatch(keyword, nested));
  }

  @Test
  void doesNotMatchWhenNeitherSideIsAContainer() {
    assertFalse(
        rule.isMatch(
            OpenSearchDataType.of(MappingType.Keyword),
            OpenSearchDataType.of(MappingType.Integer)));
    assertFalse(rule.isMatch(OpenSearchTextType.of(), OpenSearchDataType.of(MappingType.Keyword)));
  }

  @Test
  void doesNotMatchWhenBothSidesAreContainers() {
    assertFalse(rule.isMatch(object("name"), object("uid")));
    assertFalse(
        rule.isMatch(
            OpenSearchDataType.of(MappingType.Nested), OpenSearchDataType.of(MappingType.Nested)));
  }

  @Test
  void matchesTextAgainstObject() {
    assertTrue(rule.isMatch(object("name"), OpenSearchTextType.of()));
    assertTrue(rule.isMatch(OpenSearchTextType.of(), object("name")));
  }

  @Test
  void doesNotMatchNullSide() {
    assertFalse(rule.isMatch(object("name"), null));
    assertFalse(rule.isMatch(null, OpenSearchDataType.of(MappingType.Keyword)));
  }

  /** The scalar wins the path whichever side of the merge it arrives on. */
  @Test
  void resolvesToTheScalarRegardlessOfMergeOrder() {
    Map<String, OpenSearchDataType> objectFirst = new HashMap<>();
    objectFirst.put("zone", object("name"));
    rule.mergeInto("zone", OpenSearchDataType.of(MappingType.Keyword), objectFirst);
    assertEquals(MappingType.Keyword, objectFirst.get("zone").getMappingType());

    Map<String, OpenSearchDataType> scalarFirst = new HashMap<>();
    scalarFirst.put("zone", OpenSearchDataType.of(MappingType.Keyword));
    rule.mergeInto("zone", object("name"), scalarFirst);
    assertEquals(MappingType.Keyword, scalarFirst.get("zone").getMappingType());
  }

  /**
   * The object's sub-fields go with it. A row cannot hold both a scalar and a subtree at one path,
   * so a retained `path.sub` column would always read null; failing it as "field not found" is the
   * honest outcome.
   */
  @Test
  void dropsTheContainerSubFields() {
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("zone", object("name", "uid"));
    rule.mergeInto("zone", OpenSearchDataType.of(MappingType.Keyword), target);

    OpenSearchDataType merged = target.get("zone");
    assertEquals(MappingType.Keyword, merged.getMappingType());
    assertTrue(merged.getProperties().isEmpty());
  }
}
