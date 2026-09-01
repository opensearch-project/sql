/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

/**
 * Selects which indices to keep so an aggregation on a mapping conflict can still push down over a
 * clean subset. When a group field is mapped inconsistently across a wildcard pattern, some indices
 * map it to a non-aggregatable type -- the text family ({@code text}, {@code text} with a {@code
 * .keyword} sub-field, {@code match_only_text}), which the type merge collapses to bare {@code
 * text} with no doc values -- while others map it to an aggregatable type ({@code keyword}, a
 * numeric, {@code date}, {@code boolean}, {@code ip}). This drops the non-aggregatable indices,
 * keeps the aggregatable ones, and attaches a warning naming what was excluded; the caller ({@link
 * CalciteLogicalIndexScan}) re-runs pushdown over {@link Plan#keptIndices()}.
 *
 * <p>It applies only when the kept indices share one aggregatable type. If they would mix
 * incompatible aggregatable types ({@code keyword} vs {@code integer}, two numeric types) the
 * merged type is an arbitrary last-write-wins, so no subset can be kept safely; that is left to the
 * normal path (a fundamental type conflict tracked separately as #5610).
 */
final class PartialResultAggregatePushdown {

  /** Max excluded index names to spell out in the warning; the rest are summarized as "N more". */
  static final int MAX_EXCLUDED_INDICES_IN_WARNING = 5;

  private PartialResultAggregatePushdown() {}

  /**
   * The outcome of partitioning: which indices to aggregate over and the warning to attach. Absent
   * (see {@link #plan}) when partial mode cannot or need not apply.
   */
  record Plan(List<String> keptIndices, List<String> excludedIndices, Warning warning) {}

  /**
   * Decide the partial-result plan for a group key over a set of per-index mappings.
   *
   * @param bucketNames the storage fields the group keys resolve to (dotted paths); an expression
   *     key like {@code lower(city)} resolves to the field(s) it reads, e.g. {@code city}
   * @param mappings per-index field mappings, keyed by concrete index name (from {@code
   *     getIndexMappings}); the wildcard has already been resolved to concrete indices
   * @return a plan naming the kept and excluded indices plus the warning, or {@code null} when
   *     partial mode does not apply: fewer than two indices, no aggregatable subset, the kept
   *     indices would mix incompatible aggregatable types, or nothing is excluded
   */
  @Nullable
  static Plan plan(List<String> bucketNames, Map<String, IndexMapping> mappings) {
    if (bucketNames.isEmpty() || mappings.size() < 2) {
      return null;
    }

    // Group indices by their aggregatable "compatibility signature": indices sharing a signature
    // map every group field to the same aggregatable type, so they can be aggregated together
    // without re-introducing a conflict. A null signature means the field is non-aggregatable in
    // that index (text family or absent) -- always excludable.
    Map<String, List<String>> aggregatableGroups = new LinkedHashMap<>();
    List<String> excludedIndices = new ArrayList<>();
    for (Map.Entry<String, IndexMapping> entry : mappings.entrySet()) {
      // Flatten so a nested object field (mapping tree resource -> attributes -> applicationid) is
      // keyed by its dotted path, matching the bucket field name Calcite resolved.
      Map<String, OpenSearchDataType> flatMapping =
          OpenSearchDataType.traverseAndFlatten(entry.getValue().getFieldMappings());
      String signature = resolveBucketSignature(flatMapping, bucketNames);
      if (signature == null) {
        excludedIndices.add(entry.getKey());
      } else {
        aggregatableGroups.computeIfAbsent(signature, k -> new ArrayList<>()).add(entry.getKey());
      }
    }

    // Keep the aggregatable indices only when they share exactly one type. Zero aggregatable groups
    // means partial mode can't help; more than one means incompatible aggregatable types whose
    // merged type is arbitrary -> leave it to the normal path.
    if (aggregatableGroups.size() != 1) {
      return null;
    }
    if (excludedIndices.isEmpty()) {
      return null; // homogeneous already -> pushdown would not have failed
    }

    List<String> keptIndices = aggregatableGroups.values().iterator().next();
    return new Plan(
        keptIndices, excludedIndices, buildWarning(bucketNames, excludedIndices, mappings.size()));
  }

  /**
   * A per-index compatibility signature for the grouped field(s): one token per field joined by
   * {@code |}. Two indices with equal signatures map every group field to the same aggregatable
   * type and can be aggregated together. Returns {@code null} if any field is non-aggregatable
   * here: absent, or a text-family type. A {@code text} field is non-aggregatable even with a
   * {@code .keyword} sub-field, because the type merge collapses it to bare {@code text}. Each
   * aggregatable field contributes a {@code t:TYPE} token (e.g. {@code t:keyword}, {@code
   * t:integer}).
   */
  @Nullable
  static String resolveBucketSignature(
      Map<String, OpenSearchDataType> flatMapping, List<String> bucketNames) {
    List<String> tokens = new ArrayList<>();
    for (String field : bucketNames) {
      OpenSearchDataType type = flatMapping.get(field);
      if (type == null) {
        return null; // field absent here -> not aggregatable
      }
      MappingType mappingType = type.getMappingType();
      if (mappingType == MappingType.Text || mappingType == MappingType.MatchOnlyText) {
        return null; // text family (incl. text-with-.keyword) collapses to bare text on merge
      }
      tokens.add("t:" + mappingType); // aggregatable type (keyword, numeric, date, boolean, ip)
    }
    return String.join("|", tokens);
  }

  private static Warning buildWarning(
      List<String> bucketNames, List<String> excludedIndices, int totalIndices) {
    // Sort here (not in plan): ordering only matters for a stable, readable message.
    List<String> sortedExcluded = new ArrayList<>(excludedIndices);
    sortedExcluded.sort(null);
    String message =
        String.format(
            "Results exclude %d of %d indices due to a mapping conflict on %s.",
            sortedExcluded.size(), totalIndices, bucketNames);
    String detail =
        String.format(
            "%s is not aggregatable in every queried index (mapped as text or otherwise without doc"
                + " values there), so these indices were excluded from the aggregation: %s. Map %s"
                + " as an aggregatable type across all indices to include them.",
            bucketNames,
            formatIndexList(sortedExcluded, MAX_EXCLUDED_INDICES_IN_WARNING),
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
