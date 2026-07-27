/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

/**
 * Partial-result fallback for a text/keyword mapping conflict on an aggregation's group key.
 *
 * <p>When a field is mapped as {@code keyword} in some indices of a wildcard pattern and {@code
 * text} in others, the multi-index type merge must pick a single type valid on every shard, so it
 * collapses the field to {@code text}-without-{@code .keyword}. Text has no doc values, so the
 * aggregation cannot push down and instead falls back to a per-shard document scan that opens a
 * Point-In-Time (PIT) context on every shard -- exhausting {@code search.max_open_pit_context} on a
 * wide pattern.
 *
 * <p>This class computes which indices to keep so the aggregation can still push down: it
 * partitions the matched indices by how each maps the group field, then picks a single homogeneous
 * group by a deterministic priority (keyword first) and the warning describing what was excluded.
 * The caller ({@link CalciteLogicalIndexScan}) uses {@link Plan#keptIndices()} to build a narrowed
 * scan and re-runs pushdown over it.
 *
 * <p>Only the text/keyword conflict collapses aggregation pushdown this way, so this class handles
 * that case specifically rather than a general conflict framework.
 */
final class PartialResultAggregatePushdown {

  /** Max excluded index names to spell out in the warning; the rest are summarized as "N more". */
  static final int MAX_EXCLUDED_INDICES_IN_WARNING = 5;

  private PartialResultAggregatePushdown() {}

  /** How one index maps the grouped field(s), in decreasing preference for a clean pushdown. */
  enum MappingResolution {
    /** Every group field is a bare {@code keyword} -> merges to a clean {@code keyword}. */
    KEYWORD,
    /** Every group field is aggregatable, at least one via a {@code .keyword} sub-field. */
    TEXT_WITH_KEYWORD,
    /** At least one group field is not aggregatable here (bare {@code text}, or absent). */
    NOT_AGGREGATABLE
  }

  /**
   * The outcome of partitioning: which indices to aggregate over and the warning to attach. Absent
   * (see {@link #plan}) when partial mode cannot or need not apply.
   */
  record Plan(List<String> keptIndices, List<String> excludedIndices, Warning warning) {}

  /**
   * Decide the partial-result plan for a group key over a set of per-index mappings.
   *
   * @param bucketNames the aggregation's group-by field names (dotted paths)
   * @param mappings per-index field mappings, keyed by concrete index name (from {@code
   *     getIndexMappings}); the wildcard has already been resolved to concrete indices
   * @return a plan naming the kept and excluded indices plus the warning, or {@code null} when
   *     partial mode does not apply: fewer than two indices, no aggregatable subset, or nothing
   *     excluded (the query would have pushed down normally)
   */
  @Nullable
  static Plan plan(List<String> bucketNames, Map<String, IndexMapping> mappings) {
    if (bucketNames.isEmpty() || mappings.size() < 2) {
      return null;
    }

    List<String> keywordIndices = new ArrayList<>();
    List<String> textKeywordIndices = new ArrayList<>();
    List<String> excludedIndices = new ArrayList<>();
    for (Map.Entry<String, IndexMapping> entry : mappings.entrySet()) {
      // Flatten so a nested object field (mapping tree resource -> attributes -> applicationid) is
      // keyed by its dotted path, matching the bucket field name Calcite resolved.
      Map<String, OpenSearchDataType> flatMapping =
          OpenSearchDataType.traverseAndFlatten(entry.getValue().getFieldMappings());
      switch (resolveBucketMapping(flatMapping, bucketNames)) {
        case KEYWORD -> keywordIndices.add(entry.getKey());
        case TEXT_WITH_KEYWORD -> textKeywordIndices.add(entry.getKey());
        default -> excludedIndices.add(entry.getKey());
      }
    }

    // Deterministic priority, not a count-based majority: keep the keyword group whenever one
    // exists (the canonical aggregatable representation), so the returned data never depends on how
    // many stray indices of another type match. Fall back to text-with-.keyword only when there is
    // no keyword index. A mix of the two is still a text/keyword conflict that would re-collapse,
    // so
    // we never keep both -- recovering the excluded-but-aggregatable group needs a split-and-union.
    List<String> keptIndices;
    if (!keywordIndices.isEmpty()) {
      keptIndices = keywordIndices;
      excludedIndices.addAll(textKeywordIndices);
    } else if (!textKeywordIndices.isEmpty()) {
      keptIndices = textKeywordIndices;
    } else {
      return null; // no aggregatable subset -> partial mode can't help
    }
    if (excludedIndices.isEmpty()) {
      return null; // homogeneous already -> pushdown would not have failed
    }

    excludedIndices.sort(null);
    return new Plan(
        keptIndices, excludedIndices, buildWarning(bucketNames, excludedIndices, mappings.size()));
  }

  /**
   * Resolve how one index maps all grouped fields, as the weakest resolution across them: {@link
   * MappingResolution#KEYWORD} only if every field is bare keyword; {@link
   * MappingResolution#TEXT_WITH_KEYWORD} if every field is aggregatable but at least one relies on
   * a {@code .keyword} sub-field; otherwise {@link MappingResolution#NOT_AGGREGATABLE}.
   */
  static MappingResolution resolveBucketMapping(
      Map<String, OpenSearchDataType> flatMapping, List<String> bucketNames) {
    MappingResolution combined = MappingResolution.KEYWORD;
    for (String field : bucketNames) {
      OpenSearchDataType type = flatMapping.get(field);
      if (type == null) {
        return MappingResolution.NOT_AGGREGATABLE; // field absent here -> can't aggregate cleanly
      }
      if (type.getMappingType() == MappingType.Keyword) {
        continue;
      } else if (hasKeywordSubField(type)) {
        combined = MappingResolution.TEXT_WITH_KEYWORD;
      } else {
        return MappingResolution.NOT_AGGREGATABLE;
      }
    }
    return combined;
  }

  private static boolean hasKeywordSubField(OpenSearchDataType type) {
    return type instanceof OpenSearchTextType textType
        && textType.getFields().values().stream()
            .anyMatch(f -> f.getMappingType() == MappingType.Keyword);
  }

  private static Warning buildWarning(
      List<String> bucketNames, List<String> excludedIndices, int totalIndices) {
    String message =
        String.format(
            "Results exclude %d of %d indices due to a text/keyword mapping conflict on %s.",
            excludedIndices.size(), totalIndices, bucketNames);
    String detail =
        String.format(
            "Field %s is mapped inconsistently across the queried indices, which prevents"
                + " aggregation pushdown for the whole pattern. The aggregation ran only over the"
                + " indices where the field is aggregatable; excluded indices: %s. Align the"
                + " field's mapping across all indices (e.g. map it as keyword everywhere) to"
                + " include them.",
            bucketNames, formatIndexList(excludedIndices, MAX_EXCLUDED_INDICES_IN_WARNING));
    return new Warning(Warning.TYPE_PARTIAL_RESULT, message, detail);
  }

  /**
   * Format a (sorted) index-name list for a warning: spell out up to {@code limit} names, then
   * summarize any remainder as "and N more" so the message stays readable when the excluded set is
   * large.
   */
  static String formatIndexList(List<String> indices, int limit) {
    if (indices.size() <= limit) {
      return indices.toString();
    }
    return String.format(
        "[%s, ... and %d more]",
        String.join(", ", indices.subList(0, limit)), indices.size() - limit);
  }
}
