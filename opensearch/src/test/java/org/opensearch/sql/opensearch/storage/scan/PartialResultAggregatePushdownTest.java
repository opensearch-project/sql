/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.Plan;

class PartialResultAggregatePushdownTest {

  private static final OpenSearchDataType KEYWORD_TYPE = OpenSearchDataType.of(MappingType.Keyword);
  private static final OpenSearchDataType BARE_TEXT_TYPE = OpenSearchTextType.of();
  private static final OpenSearchDataType INT_TYPE = OpenSearchDataType.of(MappingType.Integer);
  private static final OpenSearchDataType TEXT_WITH_KEYWORD_TYPE =
      OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword)));

  // ---- resolveBucketSignature ----

  @Test
  void resolveKeywordField() {
    assertEquals("kw", resolveOne(Map.of("f", KEYWORD_TYPE)), "bare keyword resolves to kw");
  }

  @Test
  void resolveTextWithKeywordField() {
    assertEquals(
        "tk",
        resolveOne(Map.of("f", TEXT_WITH_KEYWORD_TYPE)),
        "text with a .keyword sub-field is aggregatable via the sub-field");
  }

  @Test
  void resolveBareTextField() {
    assertNull(
        resolveOne(Map.of("f", BARE_TEXT_TYPE)), "bare text (no .keyword) is not aggregatable");
  }

  @Test
  void resolveAbsentField() {
    assertNull(
        PartialResultAggregatePushdown.resolveBucketSignature(Map.of(), List.of("f")),
        "a field absent from the index cannot be aggregated cleanly");
  }

  @Test
  void resolveNonTextTypeIsItsOwnAggregatableSignature() {
    assertEquals(
        "t:integer",
        resolveOne(Map.of("f", INT_TYPE)),
        "a numeric field is aggregatable, tagged by its concrete type");
  }

  @Test
  void resolveMultiFieldSignatureIsPerFieldTokens() {
    Map<String, OpenSearchDataType> mapping =
        Map.of("a", KEYWORD_TYPE, "b", TEXT_WITH_KEYWORD_TYPE);
    assertEquals(
        "kw|tk", PartialResultAggregatePushdown.resolveBucketSignature(mapping, List.of("a", "b")));
    // A bare-text field anywhere in the key makes the whole index non-aggregatable.
    Map<String, OpenSearchDataType> withBareText =
        Map.of("a", KEYWORD_TYPE, "b", TEXT_WITH_KEYWORD_TYPE, "c", BARE_TEXT_TYPE);
    assertNull(
        PartialResultAggregatePushdown.resolveBucketSignature(
            withBareText, List.of("a", "b", "c")));
  }

  // ---- plan: not-applicable cases return null ----

  @Test
  void planNullForSingleIndex() {
    assertNull(PartialResultAggregatePushdown.plan(List.of("f"), Map.of("idx", keywordIndex())));
  }

  @Test
  void planNullForEmptyBucketNames() {
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of(), Map.of("kw", keywordIndex(), "txt", bareTextIndex())));
  }

  @Test
  void planNullWhenNoConflict() {
    // Two keyword indices -> nothing excluded -> pushdown would have worked normally.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), Map.of("kw1", keywordIndex(), "kw2", keywordIndex())));
  }

  @Test
  void planNullWhenNoAggregatableSubset() {
    // Every index is bare text -> partial mode can't help.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), Map.of("txt1", bareTextIndex(), "txt2", bareTextIndex())));
  }

  // ---- plan: partitioning ----

  @Test
  void planKeepsKeywordExcludesBareText() {
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "txt", bareTextIndex()));
    assertEquals(List.of("kw"), plan.keptIndices());
    assertEquals(List.of("txt"), plan.excludedIndices());
    assertEquals(Warning.TYPE_PARTIAL_RESULT, plan.warning().getType());
  }

  @Test
  void planKeepsAggregatableTypeExcludesBareText() {
    // int vs bare text: int is aggregatable, text is not. Without partial mode this silently drops
    // (or fails on) the text docs; here we keep the int index and exclude the text one.
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("num", intIndex(), "txt", bareTextIndex()));
    assertEquals(List.of("num"), plan.keptIndices());
    assertEquals(List.of("txt"), plan.excludedIndices());
  }

  @Test
  void planNullOnKeywordVsNumericConflict() {
    // keyword vs int: both aggregatable but mutually incompatible. The merged type is arbitrary
    // (last-write-wins), so keeping either group could misread the other's values -> bail and leave
    // it to the normal path (#5610), rather than risk a materialization crash.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "num", intIndex())));
  }

  @Test
  void planNullWhenMixedStringAndNumericEvenWithBareText() {
    // keyword + int + bare text: the string/numeric mix is still ambiguous, so bail even though the
    // bare-text index alone would be excludable.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"),
            ordered("kw", keywordIndex(), "num", intIndex(), "txt", bareTextIndex())));
  }

  @Test
  void planNullOnTwoNumericTypes() {
    // int vs long: two mutually-incompatible aggregatable types, arbitrary merged type -> bail.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("i", intIndex(), "l", longIndex())));
  }

  @Test
  void planKeepsKeywordEvenWhenTextWithKeywordOutnumbersIt() {
    // 1 keyword vs 3 text-with-.keyword. Keyword-first keeps the single keyword index; a count
    // majority would have kept the 3.
    Map<String, IndexMapping> mappings =
        ordered(
            "kw", keywordIndex(),
            "tk1", textWithKeywordIndex(),
            "tk2", textWithKeywordIndex(),
            "tk3", textWithKeywordIndex());
    Plan plan = PartialResultAggregatePushdown.plan(List.of("f"), mappings);
    assertEquals(List.of("kw"), plan.keptIndices());
    assertEquals(List.of("tk1", "tk2", "tk3"), plan.excludedIndices());
  }

  @Test
  void planFallsBackToTextWithKeywordWhenNoKeywordIndex() {
    Map<String, IndexMapping> mappings =
        ordered("tk", textWithKeywordIndex(), "txt", bareTextIndex());
    Plan plan = PartialResultAggregatePushdown.plan(List.of("f"), mappings);
    assertEquals(List.of("tk"), plan.keptIndices());
    assertEquals(List.of("txt"), plan.excludedIndices());
  }

  @Test
  void planExcludedIndicesAreSorted() {
    Map<String, IndexMapping> mappings =
        ordered(
            "kw", keywordIndex(),
            "txt-c", bareTextIndex(),
            "txt-a", bareTextIndex(),
            "txt-b", bareTextIndex());
    Plan plan = PartialResultAggregatePushdown.plan(List.of("f"), mappings);
    assertEquals(List.of("txt-a", "txt-b", "txt-c"), plan.excludedIndices());
  }

  // ---- formatIndexList ----

  @Test
  void formatShortListInFull() {
    assertEquals(
        "[a, b, c]", PartialResultAggregatePushdown.formatIndexList(List.of("a", "b", "c"), 5));
  }

  @Test
  void formatLongListTruncated() {
    List<String> many =
        IntStream.rangeClosed(1, 8).mapToObj(i -> "idx" + i).collect(Collectors.toList());
    String formatted = PartialResultAggregatePushdown.formatIndexList(many, 5);
    assertTrue(formatted.contains("idx1"), "spells out the first few");
    assertTrue(formatted.contains("idx5"), "spells out up to the cap");
    assertTrue(formatted.contains("and 3 more"), "summarizes the remainder");
    assertTrue(!formatted.contains("idx6"), "does not list beyond the cap");
  }

  // ---- warning content ----

  @Test
  void warningNamesFieldExcludedIndicesAndCount() {
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "txt", bareTextIndex()));
    Warning w = plan.warning();
    assertTrue(w.getMessage().contains("1 of 2 indices"), "message reports the excluded count");
    assertTrue(w.getMessage().contains("f"), "message names the field");
    assertTrue(w.getDetail().contains("txt"), "detail names the excluded index");
  }

  // ---- helpers ----

  private static String resolveOne(Map<String, OpenSearchDataType> mapping) {
    return PartialResultAggregatePushdown.resolveBucketSignature(mapping, List.of("f"));
  }

  private static IndexMapping keywordIndex() {
    return new IndexMapping(Map.of("f", KEYWORD_TYPE));
  }

  private static IndexMapping bareTextIndex() {
    return new IndexMapping(Map.of("f", BARE_TEXT_TYPE));
  }

  private static IndexMapping textWithKeywordIndex() {
    return new IndexMapping(Map.of("f", TEXT_WITH_KEYWORD_TYPE));
  }

  private static IndexMapping intIndex() {
    return new IndexMapping(Map.of("f", INT_TYPE));
  }

  private static IndexMapping longIndex() {
    return new IndexMapping(Map.of("f", OpenSearchDataType.of(MappingType.Long)));
  }

  /** Build an insertion-ordered map so kept/excluded assertions are deterministic. */
  private static Map<String, IndexMapping> ordered(Object... keyThenValue) {
    Map<String, IndexMapping> map = new LinkedHashMap<>();
    for (int i = 0; i < keyThenValue.length; i += 2) {
      map.put((String) keyThenValue[i], (IndexMapping) keyThenValue[i + 1]);
    }
    return map;
  }
}
