/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import java.io.IOException;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.JVM)
public class CalcitePPLBig5IT extends PPLBig5IT {
  private boolean initialized = false;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    // warm-up
    if (!initialized) {
      executeQuery("source=big5");
      executeQuery("source=big5 | join on 1=1 big5"); // trigger non-pushdown
      executeQuery("source=big5");
      initialized = true;
    }
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void asc_sort_timestamp_can_match_shortcut() throws IOException {
    super.asc_sort_timestamp_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void asc_sort_timestamp_no_can_match_shortcut() throws IOException {
    super.asc_sort_timestamp_no_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void desc_sort_timestamp_can_match_shortcut() throws IOException {
    super.desc_sort_timestamp_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void desc_sort_timestamp_no_can_match_shortcut() throws IOException {
    super.desc_sort_timestamp_no_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void keyword_in_range() throws IOException {
    super.keyword_in_range();
  }

  @Ignore("Relevance fields expression is unsupported in Calcite")
  @Override
  @Test
  public void query_string_on_message() throws IOException {
    super.query_string_on_message();
  }

  @Ignore("Relevance fields expression is unsupported in Calcite")
  @Override
  @Test
  public void query_string_on_message_filtered() throws IOException {
    super.query_string_on_message_filtered();
  }

  @Ignore("Relevance fields expression is unsupported in Calcite")
  @Override
  @Test
  public void query_string_on_message_filtered_sorted_num() throws IOException {
    super.query_string_on_message_filtered_sorted_num();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void sort_keyword_can_match_shortcut() throws IOException {
    super.sort_keyword_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void sort_keyword_no_can_match_shortcut() throws IOException {
    super.sort_keyword_no_can_match_shortcut();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void sort_numeric_asc_with_match() throws IOException {
    super.sort_numeric_asc_with_match();
  }

  @Ignore("Cannot resolve function: MATCH")
  @Override
  @Test
  public void sort_numeric_desc_with_match() throws IOException {
    super.sort_numeric_desc_with_match();
  }
}
