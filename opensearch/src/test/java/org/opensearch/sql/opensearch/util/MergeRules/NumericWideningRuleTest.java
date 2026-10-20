/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchAliasType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

class NumericWideningRuleTest {

  private final NumericWideningRule rule = new NumericWideningRule();

  @Test
  void testMatchDifferentNumericTypes() {
    assertTrue(rule.isMatch(type(MappingType.Integer), type(MappingType.Long)));
    assertTrue(rule.isMatch(type(MappingType.Double), type(MappingType.Long)));
    assertTrue(rule.isMatch(type(MappingType.HalfFloat), type(MappingType.Float)));
    assertTrue(rule.isMatch(type(MappingType.ScaledFloat), type(MappingType.Double)));
  }

  @Test
  void testNoMatchSameNumericType() {
    assertFalse(rule.isMatch(type(MappingType.HalfFloat), type(MappingType.HalfFloat)));
    assertFalse(rule.isMatch(type(MappingType.ScaledFloat), type(MappingType.ScaledFloat)));
  }

  @Test
  void testNoMatchNumericAndKeyword() {
    assertFalse(rule.isMatch(type(MappingType.Long), type(MappingType.Keyword)));
    assertFalse(rule.isMatch(type(MappingType.Keyword), type(MappingType.Long)));
  }

  @Test
  void testNoMatchAliasWithoutMappingType() {
    OpenSearchDataType alias = new OpenSearchAliasType("g", type(MappingType.Integer));
    assertFalse(rule.isMatch(alias, type(MappingType.Long)));
    assertFalse(rule.isMatch(type(MappingType.Long), alias));
  }

  @Test
  void testNoMatchNullSourceOrTarget() {
    assertFalse(rule.isMatch(null, type(MappingType.Long)));
    assertFalse(rule.isMatch(type(MappingType.Long), null));
  }

  @Test
  void testIntegerTypesMergeToTheWiderOne() {
    assertMergesTo(MappingType.Short, MappingType.Byte, MappingType.Short);
    assertMergesTo(MappingType.Integer, MappingType.Short, MappingType.Integer);
    assertMergesTo(MappingType.Long, MappingType.Integer, MappingType.Long);
    assertMergesTo(MappingType.Long, MappingType.Byte, MappingType.Long);
  }

  @Test
  void testHalfFloatAndFloatMergeToFloat() {
    assertMergesTo(MappingType.Float, MappingType.HalfFloat, MappingType.Float);
  }

  @Test
  void testOtherFloatingPointPairsMergeToDouble() {
    assertMergesTo(MappingType.Double, MappingType.Float, MappingType.Double);
    assertMergesTo(MappingType.Double, MappingType.ScaledFloat, MappingType.Double);
    assertMergesTo(MappingType.Double, MappingType.HalfFloat, MappingType.ScaledFloat);
  }

  @Test
  void testIntegerAndFloatingPointMergeToDouble() {
    assertMergesTo(MappingType.Double, MappingType.Long, MappingType.Double);
    assertMergesTo(MappingType.Double, MappingType.Long, MappingType.HalfFloat);
    assertMergesTo(MappingType.Double, MappingType.Integer, MappingType.Float);
    assertMergesTo(MappingType.Double, MappingType.Byte, MappingType.ScaledFloat);
  }

  @Test
  void testMergeHelperGivesOneTypeInEveryIndexOrder() {
    assertEveryOrderMergesTo(
        MappingType.Double, MappingType.Byte, MappingType.HalfFloat, MappingType.Long);
    assertEveryOrderMergesTo(
        MappingType.Long, MappingType.Byte, MappingType.Short, MappingType.Long);
    assertEveryOrderMergesTo(
        MappingType.Float, MappingType.HalfFloat, MappingType.Float, MappingType.HalfFloat);
  }

  @Test
  void testMergeHelperSendsAliasSourceToLatestRule() {
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("f", type(MappingType.Long));
    Map<String, OpenSearchDataType> source = new HashMap<>();
    source.put("f", new OpenSearchAliasType("g", type(MappingType.Integer)));

    MergeRuleHelper.merge(target, source);

    assertInstanceOf(OpenSearchAliasType.class, target.get("f"));
  }

  @Test
  void testMergeHelperSendsAliasTargetToLatestRule() {
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("f", new OpenSearchAliasType("g", type(MappingType.Integer)));
    Map<String, OpenSearchDataType> source = new HashMap<>();
    source.put("f", type(MappingType.Long));

    MergeRuleHelper.merge(target, source);

    assertEquals(MappingType.Long, target.get("f").getMappingType());
  }

  @Test
  void testMergeHelperWidensNumericSubfieldOfObject() {
    assertEquals(MappingType.Double, mergeSubfieldInOrder("long", "double"));
    assertEquals(MappingType.Double, mergeSubfieldInOrder("double", "long"));
  }

  /** Merges the two types through MergeRuleHelper in both orders. */
  private static void assertMergesTo(MappingType expected, MappingType a, MappingType b) {
    assertEquals(expected, mergeInOrder(List.of(a, b)), a + " then " + b);
    assertEquals(expected, mergeInOrder(List.of(b, a)), b + " then " + a);
  }

  private static void assertEveryOrderMergesTo(
      MappingType expected, MappingType a, MappingType b, MappingType c) {
    for (List<MappingType> order :
        List.of(
            List.of(a, b, c),
            List.of(a, c, b),
            List.of(b, a, c),
            List.of(b, c, a),
            List.of(c, a, b),
            List.of(c, b, a))) {
      assertEquals(expected, mergeInOrder(order), order.toString());
    }
  }

  /** Merges one index mapping per type, in order, the way getFieldTypes merges indices. */
  private static MappingType mergeInOrder(List<MappingType> order) {
    Map<String, OpenSearchDataType> target = new HashMap<>();
    for (MappingType mappingType : order) {
      Map<String, OpenSearchDataType> source = new HashMap<>();
      source.put("f", type(mappingType));
      MergeRuleHelper.merge(target, source);
    }
    return target.get("f").getMappingType();
  }

  /** Merges an object whose subfield n has the first type with one whose n has the second. */
  private static MappingType mergeSubfieldInOrder(
      String targetSubfieldType, String sourceSubfieldType) {
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("o", objectWithSubfield(targetSubfieldType));
    Map<String, OpenSearchDataType> source = new HashMap<>();
    source.put("o", objectWithSubfield(sourceSubfieldType));
    MergeRuleHelper.merge(target, source);
    return target.get("o").getProperties().get("n").getMappingType();
  }

  private static OpenSearchDataType objectWithSubfield(String subfieldType) {
    return OpenSearchDataType.of(
        MappingType.Object, Map.of("properties", Map.of("n", Map.of("type", subfieldType))));
  }

  private static OpenSearchDataType type(MappingType mappingType) {
    return OpenSearchDataType.of(mappingType);
  }
}
