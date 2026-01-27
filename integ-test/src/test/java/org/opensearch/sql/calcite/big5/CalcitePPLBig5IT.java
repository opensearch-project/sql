/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class CalcitePPLBig5IT extends PPLBig5IT {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void bin_bins() throws IOException {
    String ppl = sanitize(loadExpectedQuery("bin_bins.ppl"));
    timing(summary, "bin_bins", ppl);
  }

  @Test
  public void bin_span_log() throws IOException {
    String ppl = sanitize(loadExpectedQuery("bin_span_log.ppl"));
    timing(summary, "bin_span_log", ppl);
  }

  @Test
  public void bin_span_time() throws IOException {
    String ppl = sanitize(loadExpectedQuery("bin_span_time.ppl"));
    timing(summary, "bin_span_time", ppl);
  }

  @Test
  public void coalesce_nonexistent_field_fallback() throws IOException {
    String ppl = sanitize(loadExpectedQuery("coalesce_nonexistent_field_fallback.ppl"));
    timing(summary, "coalesce_nonexistent_field_fallback", ppl);
  }

  /**
   * Tests regex-based field extraction and transformation using rex command. Validates that the
   * Calcite plan correctly handles regex patterns.
   */
  @Test
  public void rex_regex_transformation() throws IOException {
    String ppl = sanitize(loadExpectedQuery("rex_regex_transformation.ppl"));
    timing(summary, "rex_regex_transformation", ppl);
  }

  /**
   * Tests LIKE pattern matching with aggregation using script engine. Validates filtering by
   * message content and grouping results.
   */
  @Test
  public void script_engine_like_pattern_with_aggregation() throws IOException {
    String ppl = sanitize(loadExpectedQuery("script_engine_like_pattern_with_aggregation.ppl"));
    timing(summary, "script_engine_like_pattern_with_aggregation", ppl);
  }

  /**
   * Tests LIKE pattern matching with sorting and result limiting. Validates filtering by message
   * content with timestamp ordering.
   */
  @Test
  public void script_engine_like_pattern_with_sort() throws IOException {
    String ppl = sanitize(loadExpectedQuery("script_engine_like_pattern_with_sort.ppl"));
    timing(summary, "script_engine_like_pattern_with_sort", ppl);
  }

  /** Tests deduplication by metrics.size field with sorting by timestamp. */
  @Test
  public void dedup_metrics_size_field() throws IOException {
    String ppl = sanitize(loadExpectedQuery("dedup_metrics_size_field.ppl"));
    timing(summary, "dedup_metrics_size_field", ppl);
    String expected = loadExpectedPlan("big5/dedup_metrics_size_field.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }
}
