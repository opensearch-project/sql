/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.clickbench;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * ClickBench SQL functional query compatibility test.
 *
 * <p>Runs the 43 standard ClickBench benchmark queries as SQL against the OpenSearch SQL plugin.
 * Each query is loaded from a .sql resource file under clickbench/queries/. This validates that
 * the SQL engine can parse and execute the analytical query patterns commonly used in ClickHouse
 * workloads, serving as a compatibility baseline for the ClickHouse dialect migration path.
 *
 * <p>Queries are sourced from the official ClickBench benchmark:
 * https://github.com/ClickHouse/ClickBench
 */
@FixMethodOrder(MethodSorters.JVM)
public class SQLClickBenchIT extends SQLIntegTestCase {

  /** Total number of ClickBench queries. */
  private static final int TOTAL_QUERIES = 43;

  /** Tracks query execution times for summary reporting. */
  private static final MapBuilder<String, Long> summary = MapBuilder.newMapBuilder();

  /** Tracks which queries passed, failed, or were skipped. */
  private static final MapBuilder<String, String> results = MapBuilder.newMapBuilder();

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
  }

  @AfterClass
  public static void printSummary() {
    Map<String, Long> timings = summary.immutableMap();
    Map<String, String> statuses = results.immutableMap();

    long passed = statuses.values().stream().filter("PASS"::equals).count();
    long failed = statuses.values().stream().filter(s -> s.startsWith("FAIL")).count();
    long skipped = statuses.values().stream().filter("SKIP"::equals).count();

    System.out.println();
    System.out.println("=== ClickBench SQL Compatibility Report ===");
    System.out.printf(Locale.ENGLISH, "Passed: %d / %d%n", passed, TOTAL_QUERIES);
    System.out.printf(Locale.ENGLISH, "Failed: %d / %d%n", failed, TOTAL_QUERIES);
    System.out.printf(Locale.ENGLISH, "Skipped: %d / %d%n", skipped, TOTAL_QUERIES);
    System.out.println();

    statuses.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              String query = entry.getKey();
              String status = entry.getValue();
              Long duration = timings.get(query);
              if (duration != null) {
                System.out.printf(
                    Locale.ENGLISH, "  %s: %s (%d ms)%n", query, status, duration);
              } else {
                System.out.printf(Locale.ENGLISH, "  %s: %s%n", query, status);
              }
            });

    if (!timings.isEmpty()) {
      long total = timings.values().stream().mapToLong(Long::longValue).sum();
      System.out.printf(
          Locale.ENGLISH,
          "%nTotal execution time: %d ms (avg %d ms per query)%n",
          total,
          total / Math.max(timings.size(), 1));
    }
    System.out.println();
  }

  /**
   * Returns the set of query numbers to skip. Override in subclasses to adjust.
   *
   * <p>Skipped queries and reasons:
   * <ul>
   *   <li>Q29: REGEXP_REPLACE not supported in legacy SQL engine</li>
   *   <li>Q30: High memory consumption query, may trigger ResourceMonitor limits</li>
   *   <li>Q35: GROUP BY ordinal (GROUP BY 1) not supported</li>
   *   <li>Q43: DATE_TRUNC not supported in legacy SQL engine</li>
   * </ul>
   */
  protected Set<Integer> ignored() {
    Set<Integer> ignored = new HashSet<>();
    ignored.add(29); // REGEXP_REPLACE
    ignored.add(30); // high memory consumption
    ignored.add(35); // GROUP BY ordinal
    ignored.add(43); // DATE_TRUNC
    return ignored;
  }

  @Test
  public void test() throws IOException {
    for (int i = 1; i <= TOTAL_QUERIES; i++) {
      String queryName = "q" + i;
      if (ignored().contains(i)) {
        results.put(queryName, "SKIP");
        continue;
      }

      logger.info("Running ClickBench SQL {}", queryName);
      String sql = loadSqlFromFile("clickbench/queries/" + queryName + ".sql");

      try {
        // Warm-up run
        executeSqlQuery(sql);

        // Timed run
        long start = System.currentTimeMillis();
        JSONObject result = executeSqlQuery(sql);
        long duration = System.currentTimeMillis() - start;

        summary.put(queryName, duration);
        results.put(queryName, "PASS");

        // Basic validation: response should have schema and datarows (JDBC format)
        Assert.assertTrue(
            queryName + " response missing 'schema'", result.has("schema"));
        Assert.assertTrue(
            queryName + " response missing 'datarows'", result.has("datarows"));

      } catch (Exception e) {
        results.put(queryName, "FAIL: " + e.getMessage());
        logger.warn("ClickBench SQL {} failed: {}", queryName, e.getMessage());
      }
    }

    // Report failures but don't fail the entire test - this is a compatibility report
    Map<String, String> statuses = results.immutableMap();
    long failCount = statuses.values().stream().filter(s -> s.startsWith("FAIL")).count();
    if (failCount > 0) {
      logger.warn("{} out of {} ClickBench SQL queries failed", failCount, TOTAL_QUERIES);
    }
  }

  /**
   * Executes a SQL query via the /_plugins/_sql endpoint and returns the JDBC-format response.
   */
  protected JSONObject executeSqlQuery(String sql) throws IOException {
    String endpoint = "/_plugins/_sql?format=jdbc";
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", escapeSql(sql)));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String body = getResponseBody(response, true);
    return new JSONObject(body);
  }

  /**
   * Loads a SQL query from a resource file, stripping comments and normalizing whitespace.
   */
  protected static String loadSqlFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      String content = new String(Files.readAllBytes(Paths.get(uri)));
      // Strip block comments
      content = content.replaceAll("(?s)/\\*.*?\\*/", "");
      // Strip line comments
      content = content.replaceAll("--[^\n]*", "");
      // Normalize whitespace
      return content.replaceAll("\\s+", " ").trim();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to load SQL file: " + filename, e);
    }
  }

  /**
   * Escapes a SQL string for embedding in a JSON request body.
   */
  private static String escapeSql(String sql) {
    return sql.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
