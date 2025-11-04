/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

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
  protected static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();

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
    String ppl = sanitize(loadExpectedQuery("asc_sort_timestamp.ppl"));
    timing(summary, "asc_sort_timestamp", ppl);
    String expected = loadExpectedPlan("asc_sort_timestamp.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void asc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("asc_sort_timestamp_can_match_shortcut.ppl"));
    timing(summary, "asc_sort_timestamp_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("asc_sort_timestamp_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void asc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("asc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing(summary, "asc_sort_timestamp_no_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("asc_sort_timestamp_no_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void asc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadExpectedQuery("asc_sort_with_after_timestamp.ppl"));
    timing(summary, "asc_sort_with_after_timestamp", ppl);
    String expected = loadExpectedPlan("asc_sort_with_after_timestamp.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void composite_date_histogram_daily() throws IOException {
    String ppl = sanitize(loadExpectedQuery("composite_date_histogram_daily.ppl"));
    timing(summary, "composite_date_histogram_daily", ppl);
    String expected = loadExpectedPlan("composite_date_histogram_daily.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void composite_terms_keyword() throws IOException {
    String ppl = sanitize(loadExpectedQuery("composite_terms_keyword.ppl"));
    timing(summary, "composite_terms_keyword", ppl);
    String expected = loadExpectedPlan("composite_terms_keyword.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void composite_terms() throws IOException {
    String ppl = sanitize(loadExpectedQuery("composite_terms.ppl"));
    timing(summary, "composite_terms", ppl);
    String expected = loadExpectedPlan("composite_terms.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void date_histogram_hourly_agg() throws IOException {
    String ppl = sanitize(loadExpectedQuery("date_histogram_hourly_agg.ppl"));
    timing(summary, "date_histogram_hourly_agg", ppl);
    String expected = loadExpectedPlan("date_histogram_hourly_agg.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void date_histogram_minute_agg() throws IOException {
    String ppl = sanitize(loadExpectedQuery("date_histogram_minute_agg.ppl"));
    timing(summary, "date_histogram_minute_agg", ppl);
    String expected = loadExpectedPlan("date_histogram_minute_agg.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void test_default() throws IOException {
    String ppl = sanitize(loadExpectedQuery("default.ppl"));
    timing(summary, "default", ppl);
    String expected = loadExpectedPlan("default.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void desc_sort_timestamp() throws IOException {
    String ppl = sanitize(loadExpectedQuery("desc_sort_timestamp.ppl"));
    timing(summary, "desc_sort_timestamp", ppl);
    String expected = loadExpectedPlan("desc_sort_timestamp.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void desc_sort_timestamp_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("desc_sort_timestamp_can_match_shortcut.ppl"));
    timing(summary, "desc_sort_timestamp_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("desc_sort_timestamp_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void desc_sort_timestamp_no_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("desc_sort_timestamp_no_can_match_shortcut.ppl"));
    timing(summary, "desc_sort_timestamp_no_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("desc_sort_timestamp_no_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void desc_sort_with_after_timestamp() throws IOException {
    String ppl = sanitize(loadExpectedQuery("desc_sort_with_after_timestamp.ppl"));
    timing(summary, "desc_sort_with_after_timestamp", ppl);
    String expected = loadExpectedPlan("desc_sort_with_after_timestamp.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void keyword_in_range() throws IOException {
    String ppl = sanitize(loadExpectedQuery("keyword_in_range.ppl"));
    timing(summary, "keyword_in_range", ppl);
    String expected = loadExpectedPlan("keyword_in_range.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void keyword_terms() throws IOException {
    String ppl = sanitize(loadExpectedQuery("keyword_terms.ppl"));
    timing(summary, "keyword_terms", ppl);
    String expected = loadExpectedPlan("keyword_terms.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void keyword_terms_low_cardinality() throws IOException {
    String ppl = sanitize(loadExpectedQuery("keyword_terms_low_cardinality.ppl"));
    timing(summary, "keyword_terms_low_cardinality", ppl);
    String expected = loadExpectedPlan("keyword_terms_low_cardinality.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void multi_terms_keyword() throws IOException {
    String ppl = sanitize(loadExpectedQuery("multi_terms_keyword.ppl"));
    timing(summary, "multi_terms_keyword", ppl);
    String expected = loadExpectedPlan("multi_terms_keyword.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void query_string_on_message() throws IOException {
    String ppl = sanitize(loadExpectedQuery("query_string_on_message.ppl"));
    timing(summary, "query_string_on_message", ppl);
    String expected = loadExpectedPlan("query_string_on_message.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void query_string_on_message_filtered() throws IOException {
    String ppl = sanitize(loadExpectedQuery("query_string_on_message_filtered.ppl"));
    timing(summary, "query_string_on_message_filtered", ppl);
    String expected = loadExpectedPlan("query_string_on_message_filtered.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void query_string_on_message_filtered_sorted_num() throws IOException {
    String ppl = sanitize(loadExpectedQuery("query_string_on_message_filtered_sorted_num.ppl"));
    timing(summary, "query_string_on_message_filtered_sorted_num", ppl);
    String expected = loadExpectedPlan("query_string_on_message_filtered_sorted_num.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range.ppl"));
    timing(summary, "range", ppl);
    String expected = loadExpectedPlan("range.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_auto_date_histo() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_auto_date_histo.ppl"));
    timing(summary, "range_auto_date_histo", ppl);
    String expected = loadExpectedPlan("range_auto_date_histo.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_auto_date_histo_with_metrics() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_auto_date_histo_with_metrics.ppl"));
    timing(summary, "range_auto_date_histo_with_metrics", ppl);
    String expected = loadExpectedPlan("range_auto_date_histo_with_metrics.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_numeric() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_numeric.ppl"));
    timing(summary, "range_numeric", ppl);
    String expected = loadExpectedPlan("range_numeric.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_field_conjunction_big_range_big_term_query() throws IOException {
    String ppl =
        sanitize(loadExpectedQuery("range_field_conjunction_big_range_big_term_query.ppl"));
    timing(summary, "range_field_conjunction_big_range_big_term_query", ppl);
    String expected = loadExpectedPlan("range_field_conjunction_big_range_big_term_query.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_field_conjunction_small_range_big_term_query() throws IOException {
    String ppl =
        sanitize(loadExpectedQuery("range_field_conjunction_small_range_big_term_query.ppl"));
    timing(summary, "range_field_conjunction_small_range_big_term_query", ppl);
    String expected = loadExpectedPlan("range_field_conjunction_small_range_big_term_query.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_field_conjunction_small_range_small_term_query() throws IOException {
    String ppl =
        sanitize(loadExpectedQuery("range_field_conjunction_small_range_small_term_query.ppl"));
    timing(summary, "range_field_conjunction_small_range_small_term_query", ppl);
    String expected = loadExpectedPlan("range_field_conjunction_small_range_small_term_query.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_field_disjunction_big_range_small_term_query() throws IOException {
    String ppl =
        sanitize(loadExpectedQuery("range_field_disjunction_big_range_small_term_query.ppl"));
    timing(summary, "range_field_disjunction_big_range_small_term_query", ppl);
    String expected = loadExpectedPlan("range_field_disjunction_big_range_small_term_query.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_with_asc_sort() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_with_asc_sort.ppl"));
    timing(summary, "range_with_asc_sort", ppl);
    String expected = loadExpectedPlan("range_with_asc_sort.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_with_desc_sort() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_with_desc_sort.ppl"));
    timing(summary, "range_with_desc_sort", ppl);
    String expected = loadExpectedPlan("range_with_desc_sort.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void scroll() throws IOException {
    String ppl = sanitize(loadExpectedQuery("scroll.ppl"));
    timing(summary, "scroll", ppl);
    String expected = loadExpectedPlan("scroll.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_keyword_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_keyword_can_match_shortcut.ppl"));
    timing(summary, "sort_keyword_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("sort_keyword_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_keyword_no_can_match_shortcut() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_keyword_no_can_match_shortcut.ppl"));
    timing(summary, "sort_keyword_no_can_match_shortcut", ppl);
    String expected = loadExpectedPlan("sort_keyword_no_can_match_shortcut.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_numeric_asc() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_numeric_asc.ppl"));
    timing(summary, "sort_numeric_asc", ppl);
    String expected = loadExpectedPlan("sort_numeric_asc.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_numeric_asc_with_match() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_numeric_asc_with_match.ppl"));
    timing(summary, "sort_numeric_asc_with_match", ppl);
    String expected = loadExpectedPlan("sort_numeric_asc_with_match.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_numeric_desc() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_numeric_desc.ppl"));
    timing(summary, "sort_numeric_desc", ppl);
    String expected = loadExpectedPlan("sort_numeric_desc.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void sort_numeric_desc_with_match() throws IOException {
    String ppl = sanitize(loadExpectedQuery("sort_numeric_desc_with_match.ppl"));
    timing(summary, "sort_numeric_desc_with_match", ppl);
    String expected = loadExpectedPlan("sort_numeric_desc_with_match.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void term() throws IOException {
    String ppl = sanitize(loadExpectedQuery("term.ppl"));
    timing(summary, "term", ppl);
    String expected = loadExpectedPlan("term.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void terms_significant_1() throws IOException {
    String ppl = sanitize(loadExpectedQuery("terms_significant_1.ppl"));
    timing(summary, "terms_significant_1", ppl);
    String expected = loadExpectedPlan("terms_significant_1.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void terms_significant_2() throws IOException {
    String ppl = sanitize(loadExpectedQuery("terms_significant_2.ppl"));
    timing(summary, "terms_significant_2", ppl);
    String expected = loadExpectedPlan("terms_significant_2.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_agg_1() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_agg_1.ppl"));
    timing(summary, "range_agg_1", ppl);
    String expected = loadExpectedPlan("range_agg_1.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void range_agg_2() throws IOException {
    String ppl = sanitize(loadExpectedQuery("range_agg_2.ppl"));
    timing(summary, "range_agg_2", ppl);
    String expected = loadExpectedPlan("range_agg_2.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void cardinality_agg_high() throws IOException {
    String ppl = sanitize(loadExpectedQuery("cardinality_agg_high.ppl"));
    timing(summary, "cardinality_agg_high", ppl);
    String expected = loadExpectedPlan("cardinality_agg_high.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void cardinality_agg_high_2() throws IOException {
    String ppl = sanitize(loadExpectedQuery("cardinality_agg_high_2.ppl"));
    timing(summary, "cardinality_agg_high_2", ppl);
    String expected = loadExpectedPlan("cardinality_agg_high_2.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  @Test
  public void cardinality_agg_low() throws IOException {
    String ppl = sanitize(loadExpectedQuery("cardinality_agg_low.ppl"));
    timing(summary, "cardinality_agg_low", ppl);
    String expected = loadExpectedPlan("cardinality_agg_low.yaml");
    assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
  }

  protected String loadExpectedQuery(String fileName) throws IOException {
    if (isCalciteEnabled()) {
      try {
        return loadFromFile("big5/queries/optimized/" + fileName);
      } catch (Exception e) {
      }
    }
    return loadFromFile("big5/queries/" + fileName);
  }
}
