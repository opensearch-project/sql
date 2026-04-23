/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.tools.RelBuilder;

@UtilityClass
public class PPLHintUtils {
  private static final String HINT_AGG_ARGUMENTS = "AGG_ARGS";
  private static final String KEY_IGNORE_NULL_BUCKET = "ignoreNullBucket";
  private static final String KEY_HAS_NESTED_AGG_CALL = "hasNestedAggCall";

  /**
   * Encoded list of dedup sort keys, one per field, in pipe-separated {@code field:ORDER} form,
   * e.g. {@code "gender:ASC|state:DESC"}. Each entry preserves the sort order from the original PPL
   * {@code sort} collation so the pushed-down {@code top_hits} can emit a full multi-field sort
   * array instead of only the first field.
   */
  private static final String KEY_DEDUP_SORT_FIELDS = "dedupSortFields";

  private static final String DEDUP_SORT_ENTRY_SEP = "|";
  private static final String DEDUP_SORT_FIELD_ORDER_SEP = ":";

  private static final Supplier<HintStrategyTable> HINT_STRATEGY_TABLE =
      Suppliers.memoize(
          () ->
              HintStrategyTable.builder()
                  .hintStrategy(
                      HINT_AGG_ARGUMENTS,
                      (hint, rel) -> {
                        return rel instanceof LogicalAggregate;
                      })
                  // add more here
                  .build());

  /**
   * Add hint to aggregate to indicate that the aggregate will ignore null value bucket. Notice, the
   * current peek of relBuilder is expected to be LogicalAggregate.
   */
  public static void addIgnoreNullBucketHintToAggregate(RelBuilder relBuilder) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    final RelHint statHint =
        RelHint.builder(HINT_AGG_ARGUMENTS).hintOption(KEY_IGNORE_NULL_BUCKET, "true").build();
    relBuilder.hints(statHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /**
   * Add hint to aggregate to indicate that the aggregate has nested agg call. Notice, the current
   * peek of relBuilder is expected to be LogicalAggregate.
   */
  public static void addNestedAggCallHintToAggregate(RelBuilder relBuilder) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    final RelHint statHint =
        RelHint.builder(HINT_AGG_ARGUMENTS).hintOption(KEY_HAS_NESTED_AGG_CALL, "true").build();
    relBuilder.hints(statHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /** Return true if the aggregate will ignore null value bucket. */
  public static boolean ignoreNullBucket(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .anyMatch(
            hint ->
                hint.hintName.equals(PPLHintUtils.HINT_AGG_ARGUMENTS)
                    && hint.kvOptions.getOrDefault(KEY_IGNORE_NULL_BUCKET, "false").equals("true"));
  }

  /** Return true if the aggregate has any nested agg call. */
  public static boolean hasNestedAggCall(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .anyMatch(
            hint ->
                hint.hintName.equals(PPLHintUtils.HINT_AGG_ARGUMENTS)
                    && hint.kvOptions
                        .getOrDefault(KEY_HAS_NESTED_AGG_CALL, "false")
                        .equals("true"));
  }

  /**
   * Add dedup sort info hint to aggregate so that AggregateAnalyzer can set top_hits sort. All
   * field collations are propagated so a multi-field PPL {@code sort} ({@code sort state, -city |
   * dedup ...}) is pushed down as a multi-field {@code top_hits} sort.
   */
  public static void addDedupSortHintToAggregate(
      RelBuilder relBuilder, RelCollation collation, java.util.List<String> fieldNames) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    String encoded = encodeDedupSortFields(collation, fieldNames);
    if (encoded.isEmpty()) {
      return;
    }
    final RelHint sortHint =
        RelHint.builder(HINT_AGG_ARGUMENTS).hintOption(KEY_DEDUP_SORT_FIELDS, encoded).build();
    relBuilder.hints(sortHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /** A single (field, order) entry from the dedup sort hint. */
  public record DedupSortKey(String field, String order) {}

  /**
   * Return the dedup sort keys from aggregate hints, preserving the order from the original PPL
   * {@code sort}. Empty list if not present.
   */
  public static List<DedupSortKey> getDedupSortKeys(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .filter(hint -> hint.hintName.equals(HINT_AGG_ARGUMENTS))
        .map(hint -> hint.kvOptions.get(KEY_DEDUP_SORT_FIELDS))
        .filter(java.util.Objects::nonNull)
        .findFirst()
        .map(PPLHintUtils::decodeDedupSortFields)
        .orElse(Collections.emptyList());
  }

  private static String encodeDedupSortFields(
      RelCollation collation, java.util.List<String> fieldNames) {
    StringBuilder sb = new StringBuilder();
    for (RelFieldCollation fc : collation.getFieldCollations()) {
      int idx = fc.getFieldIndex();
      if (idx < 0 || idx >= fieldNames.size()) {
        // Skip entries we can't map back to a field name (defensive).
        continue;
      }
      if (sb.length() > 0) {
        sb.append(DEDUP_SORT_ENTRY_SEP);
      }
      sb.append(fieldNames.get(idx))
          .append(DEDUP_SORT_FIELD_ORDER_SEP)
          .append(fc.direction.isDescending() ? "DESC" : "ASC");
    }
    return sb.toString();
  }

  private static List<DedupSortKey> decodeDedupSortFields(String encoded) {
    if (encoded == null || encoded.isEmpty()) {
      return Collections.emptyList();
    }
    List<DedupSortKey> keys = new ArrayList<>();
    for (String entry : encoded.split("\\" + DEDUP_SORT_ENTRY_SEP)) {
      int sep = entry.lastIndexOf(DEDUP_SORT_FIELD_ORDER_SEP);
      if (sep <= 0 || sep == entry.length() - 1) {
        continue;
      }
      keys.add(new DedupSortKey(entry.substring(0, sep), entry.substring(sep + 1)));
    }
    return keys;
  }
}
