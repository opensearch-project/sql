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
  void resolveBareTextFieldIsNonAggregatable() {
    assertNull(resolveOne(Map.of("f", BARE_TEXT_TYPE)), "bare text is not aggregatable");
  }

  @Test
  void resolveTextWithKeywordFieldIsNonAggregatable() {
    // A text field with a .keyword sub-field still merges to bare text across a conflict, so for
    // partial-result purposes it is non-aggregatable, same as bare text.
    assertNull(
        resolveOne(Map.of("f", TEXT_WITH_KEYWORD_TYPE)),
        "text with a .keyword sub-field is non-aggregatable (collapses to text on merge)");
  }

  @Test
  void resolveNonTextTypeIsAggregatable() {
    assertEquals(
        "t:integer",
        resolveOne(Map.of("f", INT_TYPE)),
        "a numeric field is aggregatable, tagged by its concrete type");
  }

  @Test
  void resolveAbsentField() {
    assertNull(
        PartialResultAggregatePushdown.resolveBucketSignature(Map.of(), List.of("f")),
        "a field absent from the index cannot be aggregated cleanly");
  }

  @Test
  void resolveMultiFieldSignatureIsPerFieldTokens() {
    Map<String, OpenSearchDataType> mapping = Map.of("a", KEYWORD_TYPE, "b", INT_TYPE);
    assertEquals(
        "kw|t:integer",
        PartialResultAggregatePushdown.resolveBucketSignature(mapping, List.of("a", "b")));
    // A text field anywhere in the key makes the whole index non-aggregatable.
    Map<String, OpenSearchDataType> withText =
        Map.of("a", KEYWORD_TYPE, "b", INT_TYPE, "c", BARE_TEXT_TYPE);
    assertNull(
        PartialResultAggregatePushdown.resolveBucketSignature(withText, List.of("a", "b", "c")));
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
    // Two keyword indices -> one signature, nothing excluded -> pushdown would have worked.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), Map.of("kw1", keywordIndex(), "kw2", keywordIndex())));
  }

  @Test
  void planNullWhenNoAggregatableSubset() {
    // Every index is text (bare or text-with-.keyword) -> nothing aggregatable to keep.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), Map.of("txt", bareTextIndex(), "tk", textWithKeywordIndex())));
  }

  @Test
  void planNullWhenKeptWouldMixIncompatibleTypes() {
    // keyword vs int: two aggregatable signatures, arbitrary merged type -> bail (#5610).
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "num", intIndex())));
    // int vs long: two numeric signatures -> bail.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("i", intIndex(), "l", longIndex())));
    // keyword + int + text: still mixes keyword and int, even though the text index alone is
    // excludable -> bail.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"),
            ordered("kw", keywordIndex(), "num", intIndex(), "txt", bareTextIndex())));
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
  void planKeepsKeywordExcludesTextWithKeyword() {
    // text-with-.keyword is non-aggregatable for partial purposes, so keyword is kept and the
    // text-with-.keyword index is excluded.
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "tk", textWithKeywordIndex()));
    assertEquals(List.of("kw"), plan.keptIndices());
    assertEquals(List.of("tk"), plan.excludedIndices());
  }

  @Test
  void planKeepsNumericExcludesText() {
    // int vs bare text: int is aggregatable, text is not -> keep the int index, exclude text.
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("num", intIndex(), "txt", bareTextIndex()));
    assertEquals(List.of("num"), plan.keptIndices());
    assertEquals(List.of("txt"), plan.excludedIndices());
  }

  @Test
  void planKeepsAllKeywordIndicesExcludesText() {
    // Two keyword indices + one text index -> keep both keyword, exclude the text one.
    Plan plan =
        PartialResultAggregatePushdown.plan(
            List.of("f"),
            ordered("kw1", keywordIndex(), "kw2", keywordIndex(), "txt", bareTextIndex()));
    assertEquals(List.of("kw1", "kw2"), plan.keptIndices());
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
