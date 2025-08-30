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
}
