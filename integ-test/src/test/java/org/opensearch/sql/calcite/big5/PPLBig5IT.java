/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@FixMethodOrder(MethodSorters.JVM)
public class PPLBig5IT extends PPLIntegTestCase {
  private static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BIG5);
    disableCalcite();
  }

  @AfterClass
  public static void reset() throws IOException {
    long total = 0;
    Map<String, Long> map = summary.immutableMap();
    for (long duration : map.values()) {
      total += duration;
    }
    System.out.println("Summary:");
    map.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry ->
                System.out.printf(Locale.ENGLISH, "%s: %d ms%n", entry.getKey(), entry.getValue()));
    System.out.printf(
        Locale.ENGLISH,
        "Total %d queries succeed. Average duration: %d ms%n",
        map.size(),
        total / map.size());
    System.out.println();
  }

  @Test
  public void asc_sort_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_timestamp.ppl"));
    timing(summary, "asc_sort_timestamp", ppl);
  }

  @Test
  public void asc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_timestamp_can_match_shortcut.ppl"));
    timing(summary, "asc_sort_timestamp_can_match_shortcut", ppl);
  }

  @Test
  public void asc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/asc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing(summary, "asc_sort_timestamp_no_can_match_shortcut", ppl);
  }

  @Test
  public void asc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_with_after_timestamp.ppl"));
    timing(summary, "asc_sort_with_after_timestamp", ppl);
  }

  @Test
  public void composite_date_histogram_daily() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_date_histogram_daily.ppl"));
    timing(summary, "composite_date_histogram_daily", ppl);
  }

  @Test
  public void composite_terms_keyword() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_terms_keyword.ppl"));
    timing(summary, "composite_terms_keyword", ppl);
  }

  @Test
  public void composite_terms() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_terms.ppl"));
    timing(summary, "composite_terms", ppl);
  }

  @Test
  public void date_histogram_hourly_agg() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/date_histogram_hourly_agg.ppl"));
    timing(summary, "date_histogram_hourly_agg", ppl);
  }

  @Test
  public void date_histogram_minute_agg() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/date_histogram_minute_agg.ppl"));
    timing(summary, "date_histogram_minute_agg", ppl);
  }

  @Test
  public void test_default() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/default.ppl"));
    timing(summary, "default", ppl);
  }

  @Test
  public void desc_sort_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_timestamp.ppl"));
    timing(summary, "desc_sort_timestamp", ppl);
  }

  @Test
  public void desc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_timestamp_can_match_shortcut.ppl"));
    timing(summary, "desc_sort_timestamp_can_match_shortcut", ppl);
  }

  @Test
  public void desc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/desc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing(summary, "desc_sort_timestamp_no_can_match_shortcut", ppl);
  }

  @Test
  public void desc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_with_after_timestamp.ppl"));
    timing(summary, "desc_sort_with_after_timestamp", ppl);
  }

  @Test
  public void keyword_in_range() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_in_range.ppl"));
    timing(summary, "keyword_in_range", ppl);
  }

  @Test
  public void keyword_terms() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_terms.ppl"));
    timing(summary, "keyword_terms", ppl);
  }

  @Test
  public void keyword_terms_low_cardinality() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_terms_low_cardinality.ppl"));
    timing(summary, "keyword_terms_low_cardinality", ppl);
  }

  @Test
  public void multi_terms_keyword() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/multi_terms_keyword.ppl"));
    timing(summary, "multi_terms_keyword", ppl);
  }

  @Test
  public void query_string_on_message() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/query_string_on_message.ppl"));
    timing(summary, "query_string_on_message", ppl);
  }

  @Test
  public void query_string_on_message_filtered() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/query_string_on_message_filtered.ppl"));
    timing(summary, "query_string_on_message_filtered", ppl);
  }

  @Test
  public void query_string_on_message_filtered_sorted_num() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/query_string_on_message_filtered_sorted_num.ppl"));
    timing(summary, "query_string_on_message_filtered_sorted_num", ppl);
  }

  @Test
  public void range() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range.ppl"));
    timing(summary, "range", ppl);
  }

  @Test
  public void range_auto_date_histo() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_auto_date_histo.ppl"));
    timing(summary, "range_auto_date_histo", ppl);
  }

  @Test
  public void range_auto_date_histo_with_metrics() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_auto_date_histo_with_metrics.ppl"));
    timing(summary, "range_auto_date_histo_with_metrics", ppl);
  }

  @Test
  public void range_numeric() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_numeric.ppl"));
    timing(summary, "range_numeric", ppl);
  }

  @Test
  public void range_field_conjunction_big_range_big_term_query() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/range_field_conjunction_big_range_big_term_query.ppl"));
    timing(summary, "range_field_conjunction_big_range_big_term_query", ppl);
  }

  @Test
  public void range_field_conjunction_small_range_big_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_conjunction_small_range_big_term_query.ppl"));
    timing(summary, "range_field_conjunction_small_range_big_term_query", ppl);
  }

  @Test
  public void range_field_conjunction_small_range_small_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_conjunction_small_range_small_term_query.ppl"));
    timing(summary, "range_field_conjunction_small_range_small_term_query", ppl);
  }

  @Test
  public void range_field_disjunction_big_range_small_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_disjunction_big_range_small_term_query.ppl"));
    timing(summary, "range_field_disjunction_big_range_small_term_query", ppl);
  }

  @Test
  public void range_with_asc_sort() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_with_asc_sort.ppl"));
    timing(summary, "range_with_asc_sort", ppl);
  }

  @Test
  public void range_with_desc_sort() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_with_desc_sort.ppl"));
    timing(summary, "range_with_desc_sort", ppl);
  }

  @Test
  public void scroll() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/scroll.ppl"));
    timing(summary, "scroll", ppl);
  }

  @Test
  public void sort_keyword_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_keyword_can_match_shortcut.ppl"));
    timing(summary, "sort_keyword_can_match_shortcut", ppl);
  }

  @Test
  public void coalesce_empty_string_priority() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/coalesce_empty_string_priority.ppl"));
    timing(summary, "coalesce_empty_string_priority", ppl);
  }

  @Test
  public void coalesce_nonexistent_field_fallback() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/coalesce_nonexistent_field_fallback.ppl"));
    timing(summary, "coalesce_nonexistent_field_fallback", ppl);
  }

  @Test
  public void coalesce_numeric_fallback() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/coalesce_numeric_fallback.ppl"));
    timing(summary, "coalesce_numeric_fallback", ppl);
  }

  @Test
  public void sort_keyword_no_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_keyword_no_can_match_shortcut.ppl"));
    timing(summary, "sort_keyword_no_can_match_shortcut", ppl);
  }

  @Test
  public void sort_numeric_asc() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_asc.ppl"));
    timing(summary, "sort_numeric_asc", ppl);
  }

  @Test
  public void sort_numeric_asc_with_match() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_asc_with_match.ppl"));
    timing(summary, "sort_numeric_asc_with_match", ppl);
  }

  @Test
  public void sort_numeric_desc() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_desc.ppl"));
    timing(summary, "sort_numeric_desc", ppl);
  }

  @Test
  public void sort_numeric_desc_with_match() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_desc_with_match.ppl"));
    timing(summary, "sort_numeric_desc_with_match", ppl);
  }

  @Test
  public void term() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/term.ppl"));
    timing(summary, "term", ppl);
  }

  @Test
  public void terms_significant_1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/terms_significant_1.ppl"));
    timing(summary, "terms_significant_1", ppl);
  }

  @Test
  public void terms_significant_2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/terms_significant_2.ppl"));
    timing(summary, "terms_significant_2", ppl);
  }
}
