/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@FixMethodOrder(MethodSorters.JVM)
public class PPLBig5IT extends PPLIntegTestCase {
  private boolean initialized = false;
  private static final Map<String, Long> summary = new LinkedHashMap<>();

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BIG5);
    disableCalcite();
    // warm-up
    if (!initialized) {
      executeQuery("source=big5 | eval a = 1"); // trigger non-pushdown
      initialized = true;
    }
  }

  @AfterClass
  public static void reset() throws IOException {
    long total = 0;
    for (long duration : summary.values()) {
      total += duration;
    }
    System.out.println("Summary:");
    for (Map.Entry<String, Long> entry : summary.entrySet()) {
      System.out.printf(Locale.ENGLISH, "%s: %d ms%n", entry.getKey(), entry.getValue());
    }
    System.out.printf(
        Locale.ENGLISH,
        "Total %d queries succeed. Average duration: %d ms%n",
        summary.size(),
        total / summary.size());
    System.out.println();
    summary.clear();
  }

  protected void timing(String query, String ppl) throws IOException {
    long start = System.currentTimeMillis();
    executeQuery(ppl);
    long duration = System.currentTimeMillis() - start;
    summary.put(query, duration);
  }

  @Test
  public void asc_sort_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_timestamp.ppl"));
    timing("asc_sort_timestamp", ppl);
  }

  @Test
  public void asc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_timestamp_can_match_shortcut.ppl"));
    timing("asc_sort_timestamp_can_match_shortcut", ppl);
  }

  @Test
  public void asc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/asc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing("asc_sort_timestamp_no_can_match_shortcut", ppl);
  }

  @Test
  public void asc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/asc_sort_with_after_timestamp.ppl"));
    timing("asc_sort_with_after_timestamp", ppl);
  }

  @Test
  public void composite_date_histogram_daily() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_date_histogram_daily.ppl"));
    timing("composite_date_histogram_daily", ppl);
  }

  @Test
  public void composite_terms_keyword() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_terms_keyword.ppl"));
    timing("composite_terms_keyword", ppl);
  }

  @Test
  public void composite_terms() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/composite_terms.ppl"));
    timing("composite_terms", ppl);
  }

  @Test
  public void date_histogram_hourly_agg() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/date_histogram_hourly_agg.ppl"));
    timing("date_histogram_hourly_agg", ppl);
  }

  @Test
  public void date_histogram_minute_agg() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/date_histogram_minute_agg.ppl"));
    timing("date_histogram_minute_agg", ppl);
  }

  @Test
  public void test_default() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/default.ppl"));
    timing("default", ppl);
  }

  @Test
  public void desc_sort_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_timestamp.ppl"));
    timing("desc_sort_timestamp", ppl);
  }

  @Test
  public void desc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_timestamp_can_match_shortcut.ppl"));
    timing("desc_sort_timestamp_can_match_shortcut", ppl);
  }

  @Test
  public void desc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/desc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing("desc_sort_timestamp_no_can_match_shortcut", ppl);
  }

  @Test
  public void desc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/desc_sort_with_after_timestamp.ppl"));
    timing("desc_sort_with_after_timestamp", ppl);
  }

  @Test
  public void keyword_in_range() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_in_range.ppl"));
    timing("keyword_in_range", ppl);
  }

  @Test
  public void keyword_terms() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_terms.ppl"));
    timing("keyword_terms", ppl);
  }

  @Test
  public void keyword_terms_low_cardinality() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/keyword_terms_low_cardinality.ppl"));
    timing("keyword_terms_low_cardinality", ppl);
  }

  @Test
  public void multi_terms_keyword() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/multi_terms_keyword.ppl"));
    timing("multi_terms_keyword", ppl);
  }

  @Test
  public void query_string_on_message() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/query_string_on_message.ppl"));
    timing("query_string_on_message", ppl);
  }

  @Test
  public void query_string_on_message_filtered() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/query_string_on_message_filtered.ppl"));
    timing("query_string_on_message_filtered", ppl);
  }

  @Test
  public void query_string_on_message_filtered_sorted_num() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/query_string_on_message_filtered_sorted_num.ppl"));
    timing("query_string_on_message_filtered_sorted_num", ppl);
  }

  @Test
  public void range() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range.ppl"));
    timing("range", ppl);
  }

  @Ignore("Failed to parse request payload")
  public void range_auto_date_histo() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_auto_date_histo.ppl"));
    timing("range_auto_date_histo", ppl);
  }

  @Ignore("Failed to parse request payload")
  public void range_auto_date_histo_with_metrics() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_auto_date_histo_with_metrics.ppl"));
    timing("range_auto_date_histo_with_metrics", ppl);
  }

  @Test
  public void range_numeric() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_numeric.ppl"));
    timing("range_numeric", ppl);
  }

  @Test
  public void range_field_conjunction_big_range_big_term_query() throws IOException {
    String ppl =
        sanitize(loadFromFile("big5/queries/range_field_conjunction_big_range_big_term_query.ppl"));
    timing("range_field_conjunction_big_range_big_term_query", ppl);
  }

  @Test
  public void range_field_conjunction_small_range_big_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_conjunction_small_range_big_term_query.ppl"));
    timing("range_field_conjunction_small_range_big_term_query", ppl);
  }

  @Test
  public void range_field_conjunction_small_range_small_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_conjunction_small_range_small_term_query.ppl"));
    timing("range_field_conjunction_small_range_small_term_query", ppl);
  }

  @Test
  public void range_field_disjunction_big_range_small_term_query() throws IOException {
    String ppl =
        sanitize(
            loadFromFile("big5/queries/range_field_disjunction_big_range_small_term_query.ppl"));
    timing("range_field_disjunction_big_range_small_term_query", ppl);
  }

  @Test
  public void range_with_asc_sort() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_with_asc_sort.ppl"));
    timing("range_with_asc_sort", ppl);
  }

  @Test
  public void range_with_desc_sort() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/range_with_desc_sort.ppl"));
    timing("range_with_desc_sort", ppl);
  }

  @Test
  public void scroll() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/scroll.ppl"));
    timing("scroll", ppl);
  }

  @Test
  public void sort_keyword_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_keyword_can_match_shortcut.ppl"));
    timing("sort_keyword_can_match_shortcut", ppl);
  }

  @Test
  public void sort_keyword_no_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_keyword_no_can_match_shortcut.ppl"));
    timing("sort_keyword_no_can_match_shortcut", ppl);
  }

  @Test
  public void sort_numeric_asc() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_asc.ppl"));
    timing("sort_numeric_asc", ppl);
  }

  @Test
  public void sort_numeric_asc_with_match() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_asc_with_match.ppl"));
    timing("sort_numeric_asc_with_match", ppl);
  }

  @Test
  public void sort_numeric_desc() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_desc.ppl"));
    timing("sort_numeric_desc", ppl);
  }

  @Test
  public void sort_numeric_desc_with_match() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/sort_numeric_desc_with_match.ppl"));
    timing("sort_numeric_desc_with_match", ppl);
  }

  @Test
  public void term() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/term.ppl"));
    timing("term", ppl);
  }

  @Test
  public void terms_significant_1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/terms_significant_1.ppl"));
    timing("terms_significant_1", ppl);
  }

  @Test
  public void terms_significant_2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/terms_significant_2.ppl"));
    timing("terms_significant_2", ppl);
  }
}
