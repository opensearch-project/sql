/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

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
}
