/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.ppl.PPLIntegTestCase;

@FixMethodOrder(MethodSorters.JVM)
public class PPLClickBenchIT extends PPLIntegTestCase {
  private static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
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

  /**
   * Ignore queries that are not supported by Calcite.
   *
   * <p>q30 is ignored because it will trigger ResourceMonitory health check. TODO: should be
   * addressed by: https://github.com/opensearch-project/sql/issues/3981
   */
  protected Set<Integer> ignored() {
    return Set.of(29);
  }

  @Test
  public void test() throws IOException {
    for (int i = 1; i <= 43; i++) {
      if (ignored().contains(i)) {
        continue;
      }
      String ppl = sanitize(loadFromFile("clickbench/queries/q" + i + ".ppl"));
      timing(summary, "q" + i, ppl);
    }
  }
}
