/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.MappingResolution.KEYWORD;
import static org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.MappingResolution.NOT_AGGREGATABLE;
import static org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.MappingResolution.TEXT_WITH_KEYWORD;

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
import org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.MappingResolution;
import org.opensearch.sql.opensearch.storage.scan.PartialResultAggregatePushdown.Plan;

class PartialResultAggregatePushdownTest {

  private static final OpenSearchDataType KEYWORD_TYPE = OpenSearchDataType.of(MappingType.Keyword);
  private static final OpenSearchDataType BARE_TEXT_TYPE = OpenSearchTextType.of();
  private static final OpenSearchDataType TEXT_WITH_KEYWORD_TYPE =
      OpenSearchTextType.of(Map.of("keyword", OpenSearchDataType.of(MappingType.Keyword)));

  // ---- resolveBucketMapping ----

  @Test
  void resolveKeywordField() {
    assertEquals(
        KEYWORD, resolveOne(Map.of("f", KEYWORD_TYPE)), "bare keyword resolves to KEYWORD");
  }

  @Test
  void resolveTextWithKeywordField() {
    assertEquals(
        TEXT_WITH_KEYWORD,
        resolveOne(Map.of("f", TEXT_WITH_KEYWORD_TYPE)),
        "text with a .keyword sub-field is aggregatable via the sub-field");
  }

  @Test
  void resolveBareTextField() {
    assertEquals(
        NOT_AGGREGATABLE,
        resolveOne(Map.of("f", BARE_TEXT_TYPE)),
        "bare text (no .keyword) is not aggregatable");
  }

  @Test
  void resolveAbsentField() {
    assertEquals(
        NOT_AGGREGATABLE,
        PartialResultAggregatePushdown.resolveBucketMapping(Map.of(), List.of("f")),
        "a field absent from the index cannot be aggregated cleanly");
  }

  @Test
  void resolveNonTextTypeIsConflictingType() {
    assertEquals(
        MappingResolution.CONFLICTING_TYPE,
        resolveOne(Map.of("f", OpenSearchDataType.of(MappingType.Integer))),
        "a numeric field is a type conflict, not a text/keyword collapse");
  }

  @Test
  void resolveMultiFieldTakesWeakestResolution() {
    // One keyword + one text-with-.keyword group key -> the weaker TEXT_WITH_KEYWORD wins.
    Map<String, OpenSearchDataType> mapping =
        Map.of("a", KEYWORD_TYPE, "b", TEXT_WITH_KEYWORD_TYPE);
    assertEquals(
        TEXT_WITH_KEYWORD,
        PartialResultAggregatePushdown.resolveBucketMapping(mapping, List.of("a", "b")));
    // Add a bare-text key -> the whole index becomes NOT_AGGREGATABLE.
    Map<String, OpenSearchDataType> withBareText =
        Map.of("a", KEYWORD_TYPE, "b", TEXT_WITH_KEYWORD_TYPE, "c", BARE_TEXT_TYPE);
    assertEquals(
        NOT_AGGREGATABLE,
        PartialResultAggregatePushdown.resolveBucketMapping(withBareText, List.of("a", "b", "c")));
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

  @Test
  void planNullOnNonTextTypeConflict() {
    // keyword vs int is a type conflict, not a text/keyword collapse. The int index is
    // aggregatable, so it must not be excluded -- partial mode bails and leaves it to the normal
    // path.
    assertNull(
        PartialResultAggregatePushdown.plan(
            List.of("f"), ordered("kw", keywordIndex(), "num", intIndex())));
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

  private static MappingResolution resolveOne(Map<String, OpenSearchDataType> mapping) {
    return PartialResultAggregatePushdown.resolveBucketMapping(mapping, List.of("f"));
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
    return new IndexMapping(Map.of("f", OpenSearchDataType.of(MappingType.Integer)));
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
