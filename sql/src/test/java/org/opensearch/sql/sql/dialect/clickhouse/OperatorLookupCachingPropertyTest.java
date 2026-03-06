/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.jqwik.api.*;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;

/**
 * Property-based tests for operator lookup caching correctness in {@link ClickHouseOperatorTable}.
 *
 * <p>**Validates: Requirements 16.1**
 *
 * <p>Property 29: Operator lookup caching correctness — For any sequence of operator lookups on the
 * ClickHouseOperatorTable, repeated lookups for the same function name SHALL return the same
 * operator instance (cache hit), and concurrent lookups from multiple threads SHALL not produce
 * inconsistent results.
 *
 * <p>Uses jqwik for property-based testing with a minimum of 100 iterations per property.
 */
class OperatorLookupCachingPropertyTest {

  private final ClickHouseOperatorTable table = ClickHouseOperatorTable.INSTANCE;
  private final Set<String> registeredNames = table.getRegisteredFunctionNames();

  // -------------------------------------------------------------------------
  // Property 29: Cache consistency for registered functions
  // -------------------------------------------------------------------------

  /**
   * For any registered ClickHouse function name, looking it up multiple times returns the same
   * operator (cache consistency).
   *
   * <p>**Validates: Requirements 16.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 29: Operator lookup caching correctness")
  void repeatedLookupReturnsSameOperatorInstance(
      @ForAll("registeredFunctionNames") String funcName) {
    List<SqlOperator> first = lookup(funcName);
    List<SqlOperator> second = lookup(funcName);
    List<SqlOperator> third = lookup(funcName);

    assertFalse(first.isEmpty(), "Registered function '" + funcName + "' should resolve");
    assertEquals(first.size(), second.size(), "Repeated lookups should return same number of ops");
    assertEquals(first.size(), third.size(), "Repeated lookups should return same number of ops");

    for (int i = 0; i < first.size(); i++) {
      assertSame(
          first.get(i),
          second.get(i),
          "Repeated lookup for '" + funcName + "' should return same operator instance");
      assertSame(
          first.get(i),
          third.get(i),
          "Third lookup for '" + funcName + "' should return same operator instance");
    }
  }

  // -------------------------------------------------------------------------
  // Property 29: Cache doesn't return stale data for unregistered names
  // -------------------------------------------------------------------------

  /**
   * For any unregistered function name, looking it up returns an empty list (cache doesn't return
   * stale data).
   *
   * <p>**Validates: Requirements 16.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 29: Operator lookup caching correctness")
  void unregisteredFunctionLookupReturnsEmptyList(
      @ForAll("unregisteredFunctionNames") String funcName) {
    List<SqlOperator> first = lookup(funcName);
    List<SqlOperator> second = lookup(funcName);

    assertTrue(
        first.isEmpty(),
        "Unregistered function '" + funcName + "' should return empty list, got: " + first);
    assertTrue(
        second.isEmpty(),
        "Repeated lookup for unregistered '" + funcName + "' should still return empty list");
  }

  // -------------------------------------------------------------------------
  // Property 29: Case-insensitive lookups return the same result
  // -------------------------------------------------------------------------

  /**
   * Case-insensitive lookups return the same result (e.g., "toDateTime", "TODATETIME",
   * "todatetime" all resolve to the same operator).
   *
   * <p>**Validates: Requirements 16.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 29: Operator lookup caching correctness")
  void caseInsensitiveLookupsReturnSameOperator(
      @ForAll("registeredFunctionNames") String funcName) {
    String lower = funcName.toLowerCase(Locale.ROOT);
    String upper = funcName.toUpperCase(Locale.ROOT);
    String mixed = toMixedCase(funcName);

    List<SqlOperator> lowerResult = lookup(lower);
    List<SqlOperator> upperResult = lookup(upper);
    List<SqlOperator> mixedResult = lookup(mixed);

    assertFalse(lowerResult.isEmpty(), "Lowercase lookup for '" + lower + "' should resolve");
    assertEquals(
        lowerResult.size(),
        upperResult.size(),
        "Case variants should return same number of operators");
    assertEquals(
        lowerResult.size(),
        mixedResult.size(),
        "Case variants should return same number of operators");

    for (int i = 0; i < lowerResult.size(); i++) {
      assertSame(
          lowerResult.get(i),
          upperResult.get(i),
          "Uppercase lookup for '" + upper + "' should return same operator as lowercase");
      assertSame(
          lowerResult.get(i),
          mixedResult.get(i),
          "Mixed-case lookup for '" + mixed + "' should return same operator as lowercase");
    }
  }

  // -------------------------------------------------------------------------
  // Property 29: Concurrent lookups return consistent results
  // -------------------------------------------------------------------------

  /**
   * Concurrent lookups from multiple threads return consistent results.
   *
   * <p>**Validates: Requirements 16.1**
   */
  @Property(tries = 100)
  @Tag("Feature: clickhouse-sql-dialect, Property 29: Operator lookup caching correctness")
  void concurrentLookupsReturnConsistentResults(
      @ForAll("registeredFunctionNames") String funcName) throws InterruptedException {
    int threadCount = 8;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    CopyOnWriteArrayList<List<SqlOperator>> results = new CopyOnWriteArrayList<>();
    CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      executor.submit(
          () -> {
            try {
              startLatch.await();
              results.add(lookup(funcName));
            } catch (Throwable t) {
              errors.add(t);
            } finally {
              doneLatch.countDown();
            }
          });
    }

    // Release all threads simultaneously
    startLatch.countDown();
    assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete within 10s");
    executor.shutdown();

    assertTrue(errors.isEmpty(), "No exceptions should occur during concurrent lookups: " + errors);
    assertEquals(threadCount, results.size(), "All threads should produce results");

    // All results should be non-empty and contain the same operator instance
    List<SqlOperator> reference = results.get(0);
    assertFalse(reference.isEmpty(), "Registered function '" + funcName + "' should resolve");

    for (int t = 1; t < results.size(); t++) {
      List<SqlOperator> threadResult = results.get(t);
      assertEquals(
          reference.size(),
          threadResult.size(),
          "Thread " + t + " should return same number of operators");
      for (int i = 0; i < reference.size(); i++) {
        assertSame(
            reference.get(i),
            threadResult.get(i),
            "Thread " + t + " should return same operator instance for '" + funcName + "'");
      }
    }
  }

  // -------------------------------------------------------------------------
  // Generators
  // -------------------------------------------------------------------------

  @Provide
  Arbitrary<String> registeredFunctionNames() {
    return Arbitraries.of(new ArrayList<>(registeredNames));
  }

  @Provide
  Arbitrary<String> unregisteredFunctionNames() {
    return Arbitraries.strings()
        .alpha()
        .ofMinLength(1)
        .ofMaxLength(30)
        .filter(name -> !registeredNames.contains(name.toLowerCase(Locale.ROOT)));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private List<SqlOperator> lookup(String name) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id = new SqlIdentifier(name, SqlParserPos.ZERO);
    table.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
    return result;
  }

  /**
   * Convert a string to mixed case (alternating upper/lower).
   *
   * @param name the original string
   * @return the mixed-case version
   */
  private String toMixedCase(String name) {
    StringBuilder sb = new StringBuilder(name.length());
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      sb.append(i % 2 == 0 ? Character.toUpperCase(c) : Character.toLowerCase(c));
    }
    return sb.toString();
  }
}
