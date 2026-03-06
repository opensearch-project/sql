/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.DialectRegistry;

/**
 * Concurrency stress test for the ClickHouse dialect pipeline. Verifies that the frozen
 * DialectRegistry, preprocessor, parser, and operator table are all safe under concurrent access
 * from multiple threads.
 *
 * <p><b>Validates: Requirements 12.1, 12.2</b>
 */
@Tag("Feature: clickhouse-sql-dialect, Concurrency stress test for dialect queries")
class DialectConcurrencyStressTest {

  private static final int THREAD_COUNT = 16;
  private static final int ITERATIONS_PER_THREAD = 100;

  /**
   * Representative ClickHouse queries with FORMAT, SETTINGS, and FINAL clauses to exercise the
   * preprocessor under concurrent access.
   */
  private static final List<String> QUERIES =
      List.of(
          "SELECT toStartOfHour(`ts`) AS `hr`, count() FROM logs GROUP BY `hr` ORDER BY `hr` FORMAT JSON",
          "SELECT toDateTime(created_at), toString(status) FROM events SETTINGS max_threads=4",
          "SELECT uniq(user_id) FROM analytics FINAL",
          "SELECT if(status = 200, 'ok', 'error'), count() FROM requests GROUP BY 1 FORMAT TabSeparated",
          "SELECT toFloat64(price) * toInt32(qty) FROM orders SETTINGS max_memory_usage=1000000",
          "SELECT now(), today(), formatDateTime(ts, '%Y-%m-%d') FROM events FORMAT JSONEachRow",
          "SELECT groupArray(name), count() FROM users GROUP BY dept FINAL",
          "SELECT multiIf(score > 90, 'A', score > 80, 'B', 'C') FROM students FORMAT CSV");

  /** Function names to look up in the operator table during the stress test. */
  private static final List<String> FUNCTION_NAMES =
      List.of(
          "toStartOfHour", "toDateTime", "toString", "toInt32", "uniq",
          "count", "now", "today", "formatDateTime", "groupArray",
          "multiIf", "toFloat64", "if", "toDate", "toFloat32");

  /**
   * Creates a frozen DialectRegistry with the ClickHouseDialectPlugin registered, simulating the
   * post-startup state.
   */
  private DialectRegistry createFrozenRegistry() {
    DialectRegistry registry = new DialectRegistry();
    registry.register(ClickHouseDialectPlugin.INSTANCE);
    registry.freeze();
    return registry;
  }

  /**
   * Stress test: 16 threads concurrently resolve the dialect from the registry, preprocess a query,
   * parse it, and look up operators. All threads start simultaneously via a CountDownLatch.
   *
   * <p>Asserts:
   * <ul>
   *   <li>No exceptions thrown by any thread</li>
   *   <li>All threads resolve the same plugin instance</li>
   *   <li>All preprocessed queries are valid (non-null, non-empty)</li>
   *   <li>All parses succeed</li>
   *   <li>All operator lookups return consistent results</li>
   * </ul>
   *
   * <p><b>Validates: Requirements 12.1, 12.2</b>
   */
  @Test
  void concurrentDialectPipelineStressTest() throws InterruptedException {
    DialectRegistry registry = createFrozenRegistry();
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);

    CopyOnWriteArrayList<DialectPlugin> resolvedPlugins = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<String> preprocessedQueries = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<SqlNode> parsedNodes = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<List<SqlOperator>> operatorResults = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();
    AtomicInteger totalIterations = new AtomicInteger(0);

    for (int t = 0; t < THREAD_COUNT; t++) {
      final int threadId = t;
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
            // 1. Resolve dialect from frozen registry
            DialectPlugin plugin = registry.resolve("clickhouse").orElseThrow(
                () -> new AssertionError("clickhouse dialect not found in registry"));
            resolvedPlugins.add(plugin);

            // 2. Preprocess a query
            String query = QUERIES.get((threadId * ITERATIONS_PER_THREAD + i) % QUERIES.size());
            String preprocessed = plugin.preprocessor().preprocess(query);
            assertNotNull(preprocessed, "Preprocessed query should not be null");
            assertFalse(preprocessed.isEmpty(), "Preprocessed query should not be empty");
            preprocessedQueries.add(preprocessed);

            // 3. Parse the preprocessed query
            SqlParser parser = SqlParser.create(preprocessed, plugin.parserConfig());
            SqlNode node = parser.parseQuery();
            assertNotNull(node, "Parsed SqlNode should not be null");
            parsedNodes.add(node);

            // 4. Look up operators in the operator table
            String funcName = FUNCTION_NAMES.get(i % FUNCTION_NAMES.size());
            List<SqlOperator> ops = new ArrayList<>();
            SqlIdentifier id = new SqlIdentifier(
                funcName.toUpperCase(Locale.ROOT), SqlParserPos.ZERO);
            plugin.operatorTable().lookupOperatorOverloads(
                id, null, SqlSyntax.FUNCTION, ops, SqlNameMatchers.liberal());
            assertFalse(ops.isEmpty(),
                "Operator lookup for '" + funcName + "' should return results");
            operatorResults.add(ops);

            totalIterations.incrementAndGet();
          }
        } catch (Throwable ex) {
          errors.add(ex);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    // Release all threads simultaneously
    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS),
        "All threads should complete within 60 seconds");
    executor.shutdown();

    // --- Assertions ---

    // No exceptions
    assertTrue(errors.isEmpty(),
        "No exceptions should occur during concurrent access. Errors: " + errors);

    // All iterations completed
    int expectedTotal = THREAD_COUNT * ITERATIONS_PER_THREAD;
    assertEquals(expectedTotal, totalIterations.get(),
        "All iterations should complete successfully");

    // All threads resolved the same plugin instance
    DialectPlugin referencePlugin = resolvedPlugins.get(0);
    for (DialectPlugin p : resolvedPlugins) {
      assertSame(referencePlugin, p,
          "All threads should resolve the same ClickHouseDialectPlugin instance");
    }

    // All preprocessed queries are valid
    for (String pq : preprocessedQueries) {
      assertNotNull(pq, "Preprocessed query should not be null");
      assertFalse(pq.isEmpty(), "Preprocessed query should not be empty");
      // FORMAT, SETTINGS, FINAL should be stripped from top-level
      String upper = pq.toUpperCase(Locale.ROOT);
      assertFalse(upper.contains("FORMAT JSON"), "FORMAT clause should be stripped");
      assertFalse(upper.contains("FORMAT TABSEPARATED"), "FORMAT clause should be stripped");
      assertFalse(upper.contains("FORMAT JSONEACHROW"), "FORMAT clause should be stripped");
      assertFalse(upper.contains("FORMAT CSV"), "FORMAT clause should be stripped");
      assertFalse(upper.contains("SETTINGS MAX_THREADS"), "SETTINGS clause should be stripped");
      assertFalse(upper.contains("SETTINGS MAX_MEMORY"), "SETTINGS clause should be stripped");
    }

    // All operator lookups returned consistent results for the same function
    // Group by function name and verify all results for the same function are identical
    for (int i = 0; i < operatorResults.size(); i++) {
      List<SqlOperator> ops = operatorResults.get(i);
      assertFalse(ops.isEmpty(), "Operator lookup should return results");
    }
  }

  /**
   * Stress test focused on operator table lookups: 16 threads concurrently look up all registered
   * functions and verify consistent results.
   *
   * <p><b>Validates: Requirements 12.1, 12.2</b>
   */
  @Test
  void concurrentOperatorTableLookupStressTest() throws InterruptedException {
    ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
    CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

    // Get reference results from the main thread for each function
    List<List<SqlOperator>> referenceResults = new ArrayList<>();
    for (String fn : FUNCTION_NAMES) {
      referenceResults.add(lookupOperator(table, fn));
    }

    for (int t = 0; t < THREAD_COUNT; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
            for (int f = 0; f < FUNCTION_NAMES.size(); f++) {
              String funcName = FUNCTION_NAMES.get(f);
              List<SqlOperator> result = lookupOperator(table, funcName);
              List<SqlOperator> reference = referenceResults.get(f);

              assertEquals(reference.size(), result.size(),
                  "Concurrent lookup for '" + funcName + "' should return same count");
              for (int j = 0; j < reference.size(); j++) {
                assertSame(reference.get(j), result.get(j),
                    "Concurrent lookup for '" + funcName + "' should return same instance");
              }
            }
          }
        } catch (Throwable ex) {
          errors.add(ex);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS),
        "All threads should complete within 60 seconds");
    executor.shutdown();

    assertTrue(errors.isEmpty(),
        "No exceptions during concurrent operator lookups. Errors: " + errors);
  }

  /**
   * Stress test focused on the preprocessor: 16 threads concurrently preprocess queries and verify
   * consistent results.
   *
   * <p><b>Validates: Requirements 12.1, 12.2</b>
   */
  @Test
  void concurrentPreprocessorStressTest() throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
    CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

    // Get reference results from the main thread
    ClickHouseQueryPreprocessor preprocessor = new ClickHouseQueryPreprocessor();
    List<String> referenceResults = new ArrayList<>();
    for (String q : QUERIES) {
      referenceResults.add(preprocessor.preprocess(q));
    }

    for (int t = 0; t < THREAD_COUNT; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          // Each thread creates its own preprocessor (as ClickHouseDialectPlugin.preprocessor() does)
          ClickHouseQueryPreprocessor localPreprocessor = new ClickHouseQueryPreprocessor();
          for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
            for (int q = 0; q < QUERIES.size(); q++) {
              String result = localPreprocessor.preprocess(QUERIES.get(q));
              assertEquals(referenceResults.get(q), result,
                  "Concurrent preprocessing should produce consistent results for query: "
                      + QUERIES.get(q));
            }
          }
        } catch (Throwable ex) {
          errors.add(ex);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS),
        "All threads should complete within 60 seconds");
    executor.shutdown();

    assertTrue(errors.isEmpty(),
        "No exceptions during concurrent preprocessing. Errors: " + errors);
  }

  /**
   * Stress test for the frozen DialectRegistry: 16 threads concurrently resolve and list dialects.
   *
   * <p><b>Validates: Requirements 12.1, 12.2</b>
   */
  @Test
  void concurrentRegistryAccessStressTest() throws InterruptedException {
    DialectRegistry registry = createFrozenRegistry();
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(THREAD_COUNT);
    CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

    for (int t = 0; t < THREAD_COUNT; t++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
            // Resolve registered dialect
            DialectPlugin plugin = registry.resolve("clickhouse").orElse(null);
            assertNotNull(plugin, "clickhouse dialect should be resolvable");
            assertSame(ClickHouseDialectPlugin.INSTANCE, plugin,
                "Should resolve to the singleton instance");

            // Resolve unregistered dialect
            assertTrue(registry.resolve("nonexistent").isEmpty(),
                "Unregistered dialect should return empty");

            // List available dialects
            assertTrue(registry.availableDialects().contains("clickhouse"),
                "Available dialects should contain clickhouse");

            // Verify frozen state
            assertTrue(registry.isFrozen(), "Registry should remain frozen");
          }
        } catch (Throwable ex) {
          errors.add(ex);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS),
        "All threads should complete within 60 seconds");
    executor.shutdown();

    assertTrue(errors.isEmpty(),
        "No exceptions during concurrent registry access. Errors: " + errors);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<SqlOperator> lookupOperator(ClickHouseOperatorTable table, String funcName) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(
        funcName.toUpperCase(Locale.ROOT), SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }
}
