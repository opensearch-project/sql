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
 * Selects which indices to keep so an aggregation on a text/keyword mapping conflict can push down
 * natively. When a field is {@code keyword} in some indices of a wildcard pattern and {@code text}
 * in others, the type merge collapses it to {@code text}-without-{@code .keyword}, which is not
 * natively aggregatable. This partitions the matched indices by how each maps the group field and
 * picks one homogeneous group by keyword-first priority, plus a warning naming what was excluded;
 * the caller ({@link CalciteLogicalIndexScan}) re-runs pushdown over {@link Plan#keptIndices()}.
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

    // Keyword-first priority, not a count-based majority, so the result never depends on how many
    // indices of each type match. Never keep both groups: their mix would re-collapse to a
    // conflict.
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
            "%s is not mapped as keyword in every queried index, so these indices were excluded"
                + " from the aggregation: %s. Map %s as keyword across all indices to include"
                + " them.",
            bucketNames,
            formatIndexList(excludedIndices, MAX_EXCLUDED_INDICES_IN_WARNING),
            bucketNames);
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
