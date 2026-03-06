/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ListSqlOperatorTable;

/**
 * Property-based tests for {@link DialectRegistry} concurrent access safety.
 *
 * <p>Validates: Requirements 12.1, 12.2
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class DialectRegistryConcurrentAccessPropertyTest {

  /**
   * Property 24: Registry concurrent access safety — For any set of N concurrent threads performing
   * dialect lookups on a frozen DialectRegistry, all threads SHALL receive correct results (matching
   * the registered plugin) with no exceptions, no null returns for registered dialects, and no data
   * corruption.
   *
   * <p>Validates: Requirements 12.1, 12.2
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 24: Registry concurrent access safety")
  void concurrentLookupsOnFrozenRegistryReturnCorrectResults(
      @ForAll @IntRange(min = 10, max = 50) int threadCount) throws Exception {

    // Register multiple dialects and freeze the registry
    String[] dialectNames = {"alpha", "beta", "gamma", "delta"};
    DialectPlugin[] plugins = new DialectPlugin[dialectNames.length];
    DialectRegistry registry = new DialectRegistry();

    for (int i = 0; i < dialectNames.length; i++) {
      plugins[i] = stubPlugin(dialectNames[i]);
      registry.register(plugins[i]);
    }
    registry.freeze();

    // Use a latch so all threads start concurrently
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threadCount; t++) {
      final int threadIndex = t;
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await();

                  // Each thread performs multiple resolve() and availableDialects() calls
                  for (int iter = 0; iter < 50; iter++) {
                    // Test resolve() for each registered dialect
                    for (int d = 0; d < dialectNames.length; d++) {
                      Optional<DialectPlugin> resolved = registry.resolve(dialectNames[d]);
                      assertTrue(
                          resolved.isPresent(),
                          "Thread "
                              + threadIndex
                              + ": resolve('"
                              + dialectNames[d]
                              + "') returned empty");
                      assertSame(
                          plugins[d],
                          resolved.get(),
                          "Thread "
                              + threadIndex
                              + ": resolve('"
                              + dialectNames[d]
                              + "') returned wrong plugin");
                    }

                    // Test resolve() for unregistered dialect returns empty
                    Optional<DialectPlugin> missing = registry.resolve("nonexistent");
                    assertFalse(
                        missing.isPresent(),
                        "Thread "
                            + threadIndex
                            + ": resolve('nonexistent') should return empty");

                    // Test availableDialects() returns correct set
                    Set<String> available = registry.availableDialects();
                    assertEquals(
                        dialectNames.length,
                        available.size(),
                        "Thread "
                            + threadIndex
                            + ": availableDialects() returned wrong size");
                    for (String name : dialectNames) {
                      assertTrue(
                          available.contains(name),
                          "Thread "
                              + threadIndex
                              + ": availableDialects() missing '"
                              + name
                              + "'");
                    }
                  }
                } catch (Throwable e) {
                  errors.add(e);
                }
              }));
    }

    // Release all threads simultaneously
    startLatch.countDown();

    // Wait for all threads to complete
    for (Future<?> future : futures) {
      future.get(30, TimeUnit.SECONDS);
    }

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Executor did not terminate");

    // Assert no errors occurred in any thread
    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder("Concurrent access errors:\n");
      for (Throwable e : errors) {
        sb.append("  - ").append(e.getMessage()).append("\n");
      }
      fail(sb.toString());
    }
  }

  /** Creates a minimal stub DialectPlugin with the given dialect name. */
  private static DialectPlugin stubPlugin(String name) {
    return new DialectPlugin() {
      @Override
      public String dialectName() {
        return name;
      }

      @Override
      public QueryPreprocessor preprocessor() {
        return query -> query;
      }

      @Override
      public SqlParser.Config parserConfig() {
        return SqlParser.config();
      }

      @Override
      public SqlOperatorTable operatorTable() {
        return new ListSqlOperatorTable();
      }

      @Override
      public SqlDialect sqlDialect() {
        return SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
      }
    };
  }
}
