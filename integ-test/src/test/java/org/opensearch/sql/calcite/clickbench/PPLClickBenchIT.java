/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.util.MatcherUtils.assertYamlEqualsIgnoreId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.opensearch.monitor.GCedMemoryUsage;
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

  /** Ignore queries that are not supported. */
  protected Set<Integer> ignored() throws IOException {
    Set ignored = new HashSet();
    if (!isCalciteEnabled()) {
      // regexp_replace() is not supported in v2
      ignored.add(29);
    }
    if (!GCedMemoryUsage.initialized()) {
      // Ignore q30 when use RuntimeMemoryUsage,
      // because of too much script push down, which will cause ResourceMonitor restriction.
      ignored.add(30);
    }
    if (isCalciteEnabled()) {
      // Ignore q41 as it needs special handling
      ignored.add(41);
    }
    return ignored;
  }

  @Test
  public void test() throws IOException {
    for (int i = 1; i <= 43; i++) {
      if (ignored().contains(i)) {
        continue;
      }
      logger.info("Running Query{}", i);
      String ppl = sanitize(loadFromFile("clickbench/queries/q" + i + ".ppl"));
      // V2 gets unstable scripts, ignore them when comparing plan
      if (isCalciteEnabled()) {
        String expected = loadExpectedPlan("clickbench/q" + i + ".yaml");
        assertYamlEqualsIgnoreId(expected, explainQueryYaml(ppl));
      }
      timing(summary, "q" + i, ppl);
    }
  }

  @Test
  public void testQ41() throws IOException {
    Assume.assumeTrue(isCalciteEnabled());
    logger.info("Running Query 41");
    String ppl = sanitize(loadFromFile("clickbench/queries/q41.ppl"));
    String expected = loadExpectedPlan("clickbench/q41.yaml");
    String alternative = loadExpectedPlan("clickbench/q41_alternative.yaml");
    assertYamlEqualsIgnoreId(expected, alternative, explainQueryYaml(ppl));
    timing(summary, "q" + 41, ppl);
  }
}
