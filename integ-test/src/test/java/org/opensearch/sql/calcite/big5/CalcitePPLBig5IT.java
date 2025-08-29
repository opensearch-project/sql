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
  public void bin_aligntime() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_aligntime.ppl"));
    timing(summary, "bin_aligntime", ppl);
  }

  @Test
  public void bin_bins() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_bins.ppl"));
    timing(summary, "bin_bins", ppl);
  }

  @Test
  public void bin_default() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_default.ppl"));
    timing(summary, "bin_default", ppl);
  }

  @Test
  public void bin_minspan() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_minspan.ppl"));
    timing(summary, "bin_minspan", ppl);
  }

  @Test
  public void bin_span_log() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_span_log.ppl"));
    timing(summary, "bin_span_log", ppl);
  }

  @Test
  public void bin_span_numeric() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_span_numeric.ppl"));
    timing(summary, "bin_span_numeric", ppl);
  }

  @Test
  public void bin_span_time() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_span_time.ppl"));
    timing(summary, "bin_span_time", ppl);
  }

  @Test
  public void bin_start_end() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/bin_start_end.ppl"));
    timing(summary, "bin_start_end", ppl);
  }
}
