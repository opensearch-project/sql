/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.big5;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

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
      initialized = true;
    }
  }

  @Override
  @Ignore("Relevance fields expression is unsupported in Calcite")
  public void testTQ1() throws IOException {}

  @Override
  @Ignore("Relevance fields expression is unsupported in Calcite")
  public void testTQ2() throws IOException {}

  @Override
  @Ignore("Relevance fields expression is unsupported in Calcite")
  public void testTQ3() throws IOException {}

  @Override
  @Ignore("Relevance fields expression is unsupported in Calcite")
  public void testTQ4() throws IOException {}

  @Override
  @Test
  public void testSQ1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/SQ1.ppl"));
    timing("SQ1", ppl);
  }

  @Override
  @Test
  public void testJ1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/J1.ppl"));
    timing("J1", ppl);
  }

  @Override
  @Test
  public void testJ2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/J2.ppl"));
    timing("J2", ppl);
  }
}
