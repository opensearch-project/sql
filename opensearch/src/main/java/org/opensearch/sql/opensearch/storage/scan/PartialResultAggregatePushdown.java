/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.opensearch.sql.executor.Warning;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;

/**
 * Selects which indices to keep so an aggregation on a mapping conflict can still push down over a
 * clean subset. Handles two safe shapes of an inconsistently-mapped group field across a wildcard:
 * a text/keyword collapse ({@code keyword} in some indices, {@code text} in others -> merges to
 * non-aggregatable text), and a single aggregatable type mixed with non-aggregatable text (e.g.
 * {@code integer} vs {@code text}, which otherwise silently drops the text docs). It partitions the
 * matched indices by a compatibility signature, keeps one homogeneous group, and attaches a warning
 * naming what was excluded; the caller ({@link CalciteLogicalIndexScan}) re-runs pushdown over
 * {@link Plan#keptIndices()}, which no longer conflicts. A conflict between mutually-incompatible
 * aggregatable types ({@code keyword} vs {@code integer}, two numeric types) is left to the normal
 * path: its merged type is arbitrary, so no subset can be kept safely (a fundamental type conflict
 * tracked separately as #5610).
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
   *     partial mode does not apply: fewer than two indices, no aggregatable subset, or nothing
   *     excluded (the query would have pushed down normally)
   */
  @Nullable
  static Plan plan(List<String> bucketNames, Map<String, IndexMapping> mappings) {
    if (bucketNames.isEmpty() || mappings.size() < 2) {
      return null;
    }

    // Group indices by their aggregatable "compatibility signature": indices sharing a signature
    // map every group field to the same aggregatable type, so they can be aggregated together
    // without re-introducing a conflict. A null signature means the field is not aggregatable in
    // that index (bare text or absent) -- always excludable.
    Map<String, List<String>> aggregatableGroups = new LinkedHashMap<>();
    List<String> nonAggregatable = new ArrayList<>();
    for (Map.Entry<String, IndexMapping> entry : mappings.entrySet()) {
      // Flatten so a nested object field (mapping tree resource -> attributes -> applicationid) is
      // keyed by its dotted path, matching the bucket field name Calcite resolved.
      Map<String, OpenSearchDataType> flatMapping =
          OpenSearchDataType.traverseAndFlatten(entry.getValue().getFieldMappings());
      String signature = resolveBucketSignature(flatMapping, bucketNames);
      if (signature == null) {
        nonAggregatable.add(entry.getKey());
      } else {
        aggregatableGroups.computeIfAbsent(signature, k -> new ArrayList<>()).add(entry.getKey());
      }
    }
    if (aggregatableGroups.isEmpty()) {
      return null; // no aggregatable subset -> partial mode can't help
    }

    // Pick the one homogeneous group to keep. The narrowed scan reuses the conflict's merged row
    // type, so a kept group is only safe when its values remain representable under that type:
    //   - all-string conflict (keyword / text-with-.keyword) merges deterministically to text, and
    //     any string group reads back as text -> keep the keyword-first group;
    //   - a single aggregatable type mixed with non-aggregatable text is representable whether the
    //     conflict merged to that type or to text -> keep it;
    //   - anything else (e.g. keyword vs int, or two numeric types) has an arbitrary last-write-
    //     wins merged type, so keeping one group could misread the other's values -> bail and leave
    //     it to the normal path (a fundamental type conflict, tracked separately as #5610).
    // Priority is deterministic, never a count-based majority, so the result does not depend on how
    // many indices of each type match.
    String keptSignature;
    if (aggregatableGroups.keySet().stream()
        .allMatch(PartialResultAggregatePushdown::isStringOnly)) {
      keptSignature = aggregatableGroups.keySet().stream().min(SIGNATURE_PRIORITY).orElseThrow();
    } else if (aggregatableGroups.size() == 1) {
      keptSignature = aggregatableGroups.keySet().iterator().next();
    } else {
      return null;
    }
    List<String> keptIndices = aggregatableGroups.get(keptSignature);
    List<String> excludedIndices = new ArrayList<>(nonAggregatable);
    aggregatableGroups.forEach(
        (signature, indices) -> {
          if (!signature.equals(keptSignature)) {
            excludedIndices.addAll(indices);
          }
        });
    if (excludedIndices.isEmpty()) {
      return null; // homogeneous already -> pushdown would not have failed
    }

    excludedIndices.sort(null);
    return new Plan(
        keptIndices, excludedIndices, buildWarning(bucketNames, excludedIndices, mappings.size()));
  }

  /**
   * A per-index compatibility signature for the grouped field(s): one token per field joined by
   * {@code |}. Two indices with equal signatures map every group field to the same aggregatable
   * type and can be aggregated together. Returns {@code null} if any field is not aggregatable here
   * (bare {@code text} or absent), which forces that index into the excluded set. Tokens: {@code
   * kw} for bare keyword, {@code tk} for text with a {@code .keyword} sub-field, {@code t:TYPE} for
   * any other aggregatable type (e.g. {@code t:INTEGER}).
   */
  @Nullable
  static String resolveBucketSignature(
      Map<String, OpenSearchDataType> flatMapping, List<String> bucketNames) {
    List<String> tokens = new ArrayList<>();
    for (String field : bucketNames) {
      OpenSearchDataType type = flatMapping.get(field);
      if (type == null) {
        return null; // field absent here -> can't aggregate cleanly
      }
      if (type.getMappingType() == MappingType.Keyword) {
        tokens.add("kw");
      } else if (hasKeywordSubField(type)) {
        tokens.add("tk"); // aggregatable via the .keyword sub-field
      } else if (type.getMappingType() == MappingType.Text) {
        return null; // bare text: the text/keyword collapse -> not aggregatable
      } else {
        tokens.add("t:" + type.getMappingType()); // other aggregatable type (int, date, ...)
      }
    }
    return String.join("|", tokens);
  }

  /**
   * Deterministic priority over signatures: keyword-only wins, then other aggregatable types, then
   * text-with-{@code .keyword} last (preserving the original keyword-first behavior); ties broken
   * by the signature string so the choice is stable.
   */
  private static final Comparator<String> SIGNATURE_PRIORITY =
      Comparator.comparingInt(PartialResultAggregatePushdown::signatureRank)
          .thenComparing(Comparator.naturalOrder());

  private static int signatureRank(String signature) {
    int rank = 0;
    for (String token : signature.split("\\|")) {
      rank = Math.max(rank, tokenRank(token));
    }
    return rank;
  }

  private static int tokenRank(String token) {
    if (token.equals("kw")) {
      return 0;
    }
    if (token.equals("tk")) {
      return 2;
    }
    return 1; // t:TYPE -- a concrete aggregatable type
  }

  /** A signature is string-only when every field resolves to keyword or text-with-.keyword. */
  private static boolean isStringOnly(String signature) {
    for (String token : signature.split("\\|")) {
      if (!token.equals("kw") && !token.equals("tk")) {
        return false;
      }
    }
    return true;
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
            "Results exclude %d of %d indices due to a mapping conflict on %s.",
            excludedIndices.size(), totalIndices, bucketNames);
    String detail =
        String.format(
            "%s is not mapped consistently as one aggregatable type across every queried index, so"
                + " these indices were excluded from the aggregation: %s. Map %s to the same"
                + " aggregatable type across all indices to include them.",
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
