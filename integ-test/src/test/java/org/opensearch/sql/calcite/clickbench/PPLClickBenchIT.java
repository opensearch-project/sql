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

  /**
   * Determine which benchmark query indices to skip based on runtime capabilities.
   *
   * Adds indices for queries that are unsupported or require special runtime conditions:
   * - 29 when Calcite is disabled (regexp_replace not supported in v2),
   * - 30 when GCedMemoryUsage is not initialized (to avoid ResourceMonitor restrictions from script pushdown),
   * - 41 when Calcite is enabled (requires special handling).
   *
   * @return a set of query indices to ignore
   * @throws IOException if an I/O error occurs while checking runtime state
   */
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

  /**
   * Executes ClickBench PPL queries 1–43, validates explain plans when Calcite is enabled, and records timing.
   *
   * Skips indices returned by ignored(), loads and sanitizes each query from clickbench/queries/q{i}.ppl,
   * compares the actual explain-plan YAML to clickbench/q{i}.yaml while ignoring generated IDs when Calcite is enabled,
   * and records per-query timing in the shared summary.
   *
   * @throws IOException if a query or expected-plan file cannot be read
   */
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

  /**
   * Executes ClickBench query 41, verifies the produced explain-plan YAML matches either the primary
   * or alternative expected plan, and records the query timing.
   *
   * <p>The test is skipped when Calcite is not enabled.
   *
   * @throws IOException if reading the query or expected plan files fails
   */
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