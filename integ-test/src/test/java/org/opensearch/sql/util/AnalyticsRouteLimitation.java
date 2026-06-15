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
   * The analytics-engine storage path ({@code DataFormatAwareEngine}) does not support in-place
   * document mutation, so tests that seed state via raw {@code PUT}+{@code DELETE} can't run on
   * this route.
   */
  DOC_MUTATION(
      "Test mutates docs via PUT+DELETE, which DataFormatAwareEngine (analytics-engine storage"
          + " path) does not support.");

  private final String reason;

  AnalyticsRouteLimitation(String reason) {
    this.reason = reason;
  }

  /** Human-readable explanation surfaced as the JUnit skip message. */
  public String reason() {
    return reason;
  }
}
