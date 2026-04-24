/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

class TextKeywordConflictRuleTest {

  private final TextKeywordConflictRule rule = new TextKeywordConflictRule();

  @Test
  void testMatchTextAndKeyword() {
    OpenSearchDataType text = OpenSearchDataType.of(MappingType.Text);
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertTrue(rule.isMatch(text, keyword));
    assertTrue(rule.isMatch(keyword, text));
  }

  @Test
  void testMatchMatchOnlyTextAndKeyword() {
    OpenSearchDataType matchOnlyText = OpenSearchDataType.of(MappingType.MatchOnlyText);
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertTrue(rule.isMatch(matchOnlyText, keyword));
    assertTrue(rule.isMatch(keyword, matchOnlyText));
  }

  @Test
  void testMatchTextWithKeywordSubfieldAndTextWithout() {
    OpenSearchTextType textWithKeyword =
        OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword)));
    OpenSearchTextType textWithout = OpenSearchTextType.of();
    assertTrue(rule.isMatch(textWithKeyword, textWithout));
    assertTrue(rule.isMatch(textWithout, textWithKeyword));
  }

  @Test
  void testNoMatchSameTextWithoutSubfields() {
    OpenSearchTextType text1 = OpenSearchTextType.of();
    OpenSearchTextType text2 = OpenSearchTextType.of();
    assertFalse(rule.isMatch(text1, text2));
  }

  @Test
  void testNoMatchBothTextWithKeywordSubfields() {
    OpenSearchTextType textWithKeyword1 =
        OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword)));
    OpenSearchTextType textWithKeyword2 =
        OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword)));
    assertFalse(rule.isMatch(textWithKeyword1, textWithKeyword2));
  }

  @Test
  void testNoMatchKeywordAndKeyword() {
    OpenSearchDataType keyword1 = OpenSearchDataType.of(MappingType.Keyword);
    OpenSearchDataType keyword2 = OpenSearchDataType.of(MappingType.Keyword);
    assertFalse(rule.isMatch(keyword1, keyword2));
  }

  @Test
  void testNoMatchIntegerAndKeyword() {
    OpenSearchDataType integer = OpenSearchDataType.of(MappingType.Integer);
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertFalse(rule.isMatch(integer, keyword));
  }

  @Test
  void testNoMatchNullSource() {
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    assertFalse(rule.isMatch(null, keyword));
  }

  @Test
  void testNoMatchNullTarget() {
    OpenSearchDataType text = OpenSearchDataType.of(MappingType.Text);
    assertFalse(rule.isMatch(text, null));
  }

  @Test
  void testMergeProducesTextWithoutKeywordSubfields() {
    OpenSearchDataType keyword = OpenSearchDataType.of(MappingType.Keyword);
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("msg", keyword);

    OpenSearchDataType text = OpenSearchDataType.of(MappingType.Text);
    rule.mergeInto("msg", text, target);

    OpenSearchDataType merged = target.get("msg");
    assertInstanceOf(OpenSearchTextType.class, merged);
    OpenSearchTextType mergedText = (OpenSearchTextType) merged;
    assertTrue(mergedText.getFields().isEmpty(), "Merged type should have no keyword subfields");
  }

  @Test
  void testMergeHelperIntegration() {
    // Simulate merging two index mappings with conflicting text/keyword types
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("msg", OpenSearchDataType.of(MappingType.Keyword));
    target.put("idx", OpenSearchDataType.of(MappingType.Integer));

    Map<String, OpenSearchDataType> source = new HashMap<>();
    source.put("msg", OpenSearchDataType.of(MappingType.Text));
    source.put("idx", OpenSearchDataType.of(MappingType.Integer));

    MergeRuleHelper.merge(target, source);

    // msg should be merged to text without keyword subfields
    assertInstanceOf(OpenSearchTextType.class, target.get("msg"));
    OpenSearchTextType mergedText = (OpenSearchTextType) target.get("msg");
    assertTrue(mergedText.getFields().isEmpty());

    // idx should remain integer (same type in both, LatestRule applies)
    assertEquals(MappingType.Integer, target.get("idx").getMappingType());
  }

  @Test
  void testToKeywordSubFieldReturnsNullForMergedType() {
    // After merging text and keyword, toKeywordSubField should return null,
    // forcing SOURCE retrieval instead of DOC_VALUE
    Map<String, OpenSearchDataType> target = new HashMap<>();
    target.put("msg", OpenSearchDataType.of(MappingType.Keyword));

    Map<String, OpenSearchDataType> source = new HashMap<>();
    source.put("msg", OpenSearchDataType.of(MappingType.Text));

    MergeRuleHelper.merge(target, source);

    OpenSearchDataType mergedType = target.get("msg");
    String result = OpenSearchTextType.toKeywordSubField("msg", mergedType.getExprType());
    // Should return null because the merged text type has no keyword subfield
    assertNull(result);
  }
}
