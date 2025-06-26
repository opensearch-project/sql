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
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

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
      executeQuery("source=big5");
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
  public void testTQ1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TQ1.ppl"));
    timing("TQ1", ppl);
  }

  @Test
  public void testTQ2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TQ2.ppl"));
    timing("TQ2", ppl);
  }

  @Test
  public void testTQ3() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TQ3.ppl"));
    timing("TQ3", ppl);
  }

  @Test
  public void testTQ4() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TQ4.ppl"));
    timing("TQ4", ppl);
  }

  @Test
  public void testTQ5() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TQ5.ppl"));
    timing("TQ5", ppl);
  }

  @Test
  public void testS1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S1.ppl"));
    timing("S1", ppl);
  }

  @Test
  public void testS2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S2.ppl"));
    timing("S2", ppl);
  }

  @Test
  public void testS3() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S3.ppl"));
    timing("S3", ppl);
  }

  @Test
  public void testS4() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S4.ppl"));
    timing("S4", ppl);
  }

  @Test
  public void testS5() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S5.ppl"));
    timing("S5", ppl);
  }

  @Test
  public void testS6() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S6.ppl"));
    timing("S6", ppl);
  }

  @Test
  public void testS7() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S7.ppl"));
    timing("S7", ppl);
  }

  @Test
  public void testS8() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S8.ppl"));
    timing("S8", ppl);
  }

  @Test
  public void testS9() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S9.ppl"));
    timing("S9", ppl);
  }

  @Test
  public void testS10() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S10.ppl"));
    timing("S10", ppl);
  }

  @Test
  public void testS11() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S11.ppl"));
  }

  @Test
  public void testS12() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S12.ppl"));
    timing("S12", ppl);
  }

  @Test
  public void testS13() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/S13.ppl"));
    timing("S13", ppl);
  }

  @Test
  public void testDH1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/DH1.ppl"));
    timing("DH1", ppl);
  }

  @Test
  public void testDH2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/DH2.ppl"));
    timing("DH2", ppl);
  }

  @Test
  public void testDH3() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/DH3.ppl"));
    timing("DH3", ppl);
  }

  @Ignore("DH4 fail in client")
  public void testDH4() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/DH4.ppl"));
    timing("DH4", ppl);
  }

  @Ignore("DH5 fail in client")
  public void testDH5() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/DH5.ppl"));
    timing("DH5", ppl);
  }

  @Test
  public void testR1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R1.ppl"));
    timing("R1", ppl);
  }

  @Test
  public void testR2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R2.ppl"));
    timing("R2", ppl);
  }

  @Test
  public void testR3() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R3.ppl"));
    timing("R3", ppl);
  }

  @Test
  public void testR4() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R4.ppl"));
    timing("R4", ppl);
  }

  @Test
  public void testR5() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R5.ppl"));
    timing("R5", ppl);
  }

  @Test
  public void testR6() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R6.ppl"));
    timing("R6", ppl);
  }

  @Test
  public void testR7() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/R7.ppl"));
    timing("R7", ppl);
  }

  @Test
  public void testTA1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TA1.ppl"));
    timing("TA1", ppl);
  }

  @Test
  public void testTA2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TA2.ppl"));
    timing("TA2", ppl);
  }

  @Test
  public void testTA3() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TA3.ppl"));
    timing("TA3", ppl);
  }

  @Test
  public void testTA4() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TA4.ppl"));
    timing("TA4", ppl);
  }

  @Test
  public void testTA5() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/TA5.ppl"));
    timing("TA5", ppl);
  }

  @Ignore("v2 is unsupported")
  public void testSQ1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/SQ1.ppl"));
    timing("SQ1", ppl);
  }

  @Ignore("v2 is unsupported")
  public void testJ1() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/J1.ppl"));
    timing("J1", ppl);
  }

  @Ignore("v2 is unsupported")
  public void testJ2() throws IOException {
    String ppl = sanitize(loadFromFile("big5/queries/J2.ppl"));
    timing("J2", ppl);
  }
}
