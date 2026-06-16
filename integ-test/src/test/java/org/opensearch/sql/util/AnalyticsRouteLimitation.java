/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

/**
 * Single registry of the known behavioral divergences of the analytics-engine route
 * (parquet/composite store + DataFusion backend, activated by {@code
 * -Dtests.analytics.parquet_indices=true}). Each constant carries the reason a test is skipped on
 * that route.
 *
 * <p>Pair it with {@code PPLIntegTestCase.assumeNotAnalytics(...)} so a skip reads as {@code
 * assumeNotAnalytics(NESTED_FIELDS)} instead of a copy-pasted string literal. Keeping every reason
 * here makes the full set of analytics-route gaps greppable in one place — both for humans tracking
 * what still needs fixing and as a single block of context to hand an agent for bulk triage.
 */
public enum AnalyticsRouteLimitation {
  /**
   * The parquet/composite store has no nested-document support, so nested fields are stripped from
   * the dataset at load (#5541) and queries that reference them resolve against fields that don't
   * exist on this route.
   */
  NESTED_FIELDS(
      "Nested-field queries can't run on the analytics-engine route: the parquet/composite store"
          + " has no nested-document support, so nested fields are stripped from the dataset at"
          + " load (#5541)."),

  /**
   * Parquet-backed scans surface only mapped document fields, so the {@code _id} metadata field is
   * not exposed on this route.
   */
  ID_METADATA(
      "The analytics-engine route doesn't expose the _id metadata field (parquet-backed scans"
          + " surface only mapped document fields)."),

  /**
   * The composite dataformat's dynamic mapping gives string fields a {@code text} mapping without
   * the {@code .keyword} sub-field that standard OpenSearch adds, so exact equality ({@code =} /
   * {@code ==}) on a dynamically-mapped string silently returns no rows on the DataFusion scan.
   * Explicitly mapped {@code text}+{@code keyword} fields are unaffected.
   */
  DYNAMIC_STRING_NO_KEYWORD(
      "Exact equality (= / ==) on a dynamically-mapped string field returns no rows on the"
          + " analytics-engine route: the composite dataformat's dynamic mapping omits the .keyword"
          + " sub-field that standard OpenSearch adds, and the DataFusion scan can't match on an"
          + " analyzed text field."),

  /**
   * Exact equality ({@code =} / {@code ==}) on an explicitly {@code text}-mapped field (no {@code
   * .keyword} sub-field) returns no rows on the analytics-engine route — the DataFusion scan can't
   * match on an analyzed text field. The sibling of {@link #DYNAMIC_STRING_NO_KEYWORD} for fields
   * that are mapped {@code text} on purpose rather than by dynamic mapping. Verified directly:
   * {@code where department = 'DATA'} returns no rows while {@code like(department, 'DATA')} and
   * keyword-field equality both work.
   */
  TEXT_FIELD_EXACT_MATCH(
      "Exact equality (= / ==) on an explicitly text-mapped field (no .keyword sub-field) returns"
          + " no rows on the analytics-engine route: the DataFusion scan can't match on an analyzed"
          + " text field. Use like() or a keyword field instead."),

  /**
   * The analytics-engine storage path ({@code DataFormatAwareEngine}) does not support in-place
   * document mutation, so tests that seed state via raw {@code PUT}+{@code DELETE} can't run on
   * this route.
   */
  DOC_MUTATION(
      "Test mutates docs via PUT+DELETE, which DataFormatAwareEngine (analytics-engine storage"
          + " path) does not support."),

  /**
   * When every {@code multisearch} subsearch reads the same index, the analytics-engine route
   * applies the first subsearch's filter to all of them (each keeps its own {@code eval} label), so
   * later subsearches silently return the first subsearch's rows. Produces wrong counts/duplication
   * — the route can't be asserted against. Reproduces single-shard.
   */
  MULTISEARCH_SAME_INDEX_CONFLATION(
      "multisearch with same-index subsearches conflates on the analytics-engine route: every"
          + " subsearch executes the first subsearch's filter, so counts/rows are wrong."),

  /**
   * A {@code multisearch} over heterogeneous indices returns the merged columns in a different
   * order than the v2/Calcite path (e.g. trailing fields swapped), so row-order-sensitive
   * assertions diverge even though the values are correct.
   */
  MULTISEARCH_COLUMN_ORDER(
      "multisearch over different indices returns merged columns in a different order on the"
          + " analytics-engine route than the v2/Calcite path."),

  /**
   * Binning a time field then grouping by it ({@code bin <timefield> bins=N | stats ... by
   * <timefield>}) diverges on the analytics-engine route: the date-histogram bucket column comes
   * back typed {@code string} rather than {@code timestamp}, and the route produces a different
   * bucket set (different auto-histogram span / empty buckets not filtered) so the row counts don't
   * match the v2/Calcite path.
   */
  BIN_TIME_FIELD_BUCKETING(
      "bin on a time field then grouping by it diverges on the analytics-engine route: the bucket"
          + " column is typed string (not timestamp) and the bucket set differs from the v2/Calcite"
          + " path."),

  /**
   * {@code COALESCE} over operands that are all untyped NULL (e.g. {@code coalesce(field1, field2,
   * field3)} where every field is missing from the mapping) is rejected by the analytics-engine
   * capability registry with {@code No backend supports scalar function [COALESCE] among
   * [datafusion]}. The v2/Calcite path returns an {@code undefined}-typed null instead (#5175).
   */
  COALESCE_ALL_NULL_OPERANDS(
      "COALESCE over all-untyped-null operands is unsupported on the analytics-engine route (the"
          + " capability registry rejects it); the v2/Calcite path returns an undefined-typed"
          + " null."),

  /**
   * A {@code head N} without a stable {@code sort} returns a non-deterministic row set on the
   * analytics-engine route — raw-{@code PUT} docs land in a separate segment and can sort ahead of
   * the bulk-loaded docs. Adding a {@code sort} fixes it only when the sort key is unique over the
   * head window; when ties fall back to a nullable key, null placement diverges between the
   * v2/Calcite and analytics routes, so the row set still differs.
   */
  HEAD_WITHOUT_STABLE_SORT(
      "head N without a sort on a key that is unique over the head window is non-deterministic on"
          + " the analytics-engine route, and a nullable tiebreak orders nulls differently than the"
          + " v2/Calcite path."),

  /**
   * The {@code subsearch.maxout} cap on an {@code in}-subquery is lowered as a {@code LIMIT} on the
   * right-hand side of the semi-join ({@code LogicalSystemLimit(fetch=N, type=SUBSEARCH_MAXOUT)}).
   * The analytics-engine route does not honor that LIMIT, so the subsearch returns all rows
   * regardless of the cap. Verified: with {@code subsearch.maxout=1} an {@code id in [...]}
   * subquery still returns every matching row.
   */
  SUBSEARCH_MAXOUT_IN_SUBQUERY(
      "subsearch.maxout is not honored on the analytics-engine route: the LIMIT lowered onto the"
          + " in-subquery semi-join's right side is dropped, so the subsearch returns all rows"
          + " regardless of the cap.");

  private final String reason;

  AnalyticsRouteLimitation(String reason) {
    this.reason = reason;
  }

  /** Human-readable explanation surfaced as the JUnit skip message. */
  public String reason() {
    return reason;
  }
}
