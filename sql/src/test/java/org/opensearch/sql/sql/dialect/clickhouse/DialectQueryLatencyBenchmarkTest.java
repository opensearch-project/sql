/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Benchmark tests measuring cold-start vs warm query latency through the ClickHouse dialect
 * pipeline: preprocessing, parsing, and operator table lookup.
 *
 * <p>This is a simple JUnit-based benchmark (not JMH) that uses {@code System.nanoTime()} for
 * timing. It runs 100 warm-up iterations followed by 1000 measured iterations and prints timing
 * results for manual inspection.
 *
 * <p><b>Validates: Requirements 16.4</b>
 */
@Tag("Feature: clickhouse-sql-dialect, Benchmark: cold vs warm query latency")
class DialectQueryLatencyBenchmarkTest {

  private static final int WARMUP_ITERATIONS = 100;
  private static final int MEASURED_ITERATIONS = 1000;

  private static final ClickHouseQueryPreprocessor PREPROCESSOR =
      new ClickHouseQueryPreprocessor();
  private static final SqlParser.Config PARSER_CONFIG =
      ClickHouseDialectPlugin.INSTANCE.parserConfig();
  private static final ClickHouseOperatorTable OPERATOR_TABLE = ClickHouseOperatorTable.INSTANCE;

  /** Representative ClickHouse queries covering various function types and clause patterns. */
  private static final List<String> REPRESENTATIVE_QUERIES =
      List.of(
          "SELECT toStartOfHour(`ts`) AS `hr`, count() FROM logs GROUP BY `hr` ORDER BY `hr`",
          "SELECT toDateTime(created_at), toString(status) FROM events WHERE toInt64(id) > 100",
          "SELECT uniq(user_id), uniqExact(session_id) FROM analytics GROUP BY toStartOfDay(`ts`)",
          "SELECT if(status = 200, 'ok', 'error') AS `res`, count() FROM requests GROUP BY `res`",
          "SELECT toFloat64(response_time) FROM metrics FORMAT JSON",
          "SELECT now(), today(), formatDateTime(created_at, '%Y-%m-%d') FROM events SETTINGS max_threads=4",
          "SELECT groupArray(name), count() FROM users GROUP BY department FINAL",
          "SELECT multiIf(score > 90, 'A', score > 80, 'B', 'C') AS `grd` FROM students",
          "SELECT toFloat64(price) * toInt32(quantity) AS `total` FROM orders",
          "SELECT toStartOfMonth(`dt`), sum(toFloat64(amount)) FROM transactions GROUP BY toStartOfMonth(`dt`) ORDER BY toStartOfMonth(`dt`) LIMIT 12");

  /** Functions to look up in the operator table during the benchmark. */
  private static final List<String> FUNCTION_NAMES =
      List.of(
          "toStartOfHour", "toDateTime", "toString", "toInt64", "uniq", "uniqExact",
          "toStartOfDay", "count", "now", "today", "formatDateTime", "groupArray",
          "multiIf", "toFloat64", "toInt32", "toStartOfMonth", "quantile", "if",
          "toDate", "toFloat32");

  // -------------------------------------------------------------------------
  // Cold vs Warm: Full Pipeline
  // -------------------------------------------------------------------------

  /**
   * Measures cold-start latency (first query) vs warm latency (subsequent queries) through the
   * full dialect pipeline: preprocess → parse → operator lookup.
   */
  @Test
  void coldVsWarmFullPipelineLatency() throws SqlParseException {
    // --- Cold start: first query through the pipeline ---
    String coldQuery = REPRESENTATIVE_QUERIES.get(0);
    long coldStart = System.nanoTime();
    runPipeline(coldQuery);
    long coldNanos = System.nanoTime() - coldStart;

    // --- Warm-up phase ---
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      for (String query : REPRESENTATIVE_QUERIES) {
        runPipeline(query);
      }
    }

    // --- Measured phase ---
    long[] latencies = new long[MEASURED_ITERATIONS];
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      String query = REPRESENTATIVE_QUERIES.get(i % REPRESENTATIVE_QUERIES.size());
      long start = System.nanoTime();
      runPipeline(query);
      latencies[i] = System.nanoTime() - start;
    }

    // --- Compute statistics ---
    long warmMin = Long.MAX_VALUE;
    long warmMax = Long.MIN_VALUE;
    long warmSum = 0;
    for (long l : latencies) {
      warmMin = Math.min(warmMin, l);
      warmMax = Math.max(warmMax, l);
      warmSum += l;
    }
    double warmAvgNanos = (double) warmSum / MEASURED_ITERATIONS;

    // Sort for percentiles
    java.util.Arrays.sort(latencies);
    long warmMedian = latencies[MEASURED_ITERATIONS / 2];
    long warmP95 = latencies[(int) (MEASURED_ITERATIONS * 0.95)];
    long warmP99 = latencies[(int) (MEASURED_ITERATIONS * 0.99)];

    // --- Print results ---
    System.out.println("=== ClickHouse Dialect Pipeline Latency Benchmark ===");
    System.out.printf("Cold start (first query):  %,d ns (%.3f ms)%n", coldNanos, coldNanos / 1e6);
    System.out.printf("Warm avg (%d iters):       %,.0f ns (%.3f ms)%n",
        MEASURED_ITERATIONS, warmAvgNanos, warmAvgNanos / 1e6);
    System.out.printf("Warm median:               %,d ns (%.3f ms)%n", warmMedian, warmMedian / 1e6);
    System.out.printf("Warm min:                  %,d ns (%.3f ms)%n", warmMin, warmMin / 1e6);
    System.out.printf("Warm max:                  %,d ns (%.3f ms)%n", warmMax, warmMax / 1e6);
    System.out.printf("Warm P95:                  %,d ns (%.3f ms)%n", warmP95, warmP95 / 1e6);
    System.out.printf("Warm P99:                  %,d ns (%.3f ms)%n", warmP99, warmP99 / 1e6);
    System.out.println("====================================================");

    // --- Sanity check: warm queries should not be significantly slower than cold ---
    // Warm P99 should be no more than 10x the cold start (generous bound for CI stability)
    assertTrue(
        warmP99 <= coldNanos * 10,
        String.format(
            "Warm P99 (%,d ns) should not exceed 10x cold start (%,d ns)",
            warmP99, coldNanos));
  }

  // -------------------------------------------------------------------------
  // Cold vs Warm: Preprocessing Only
  // -------------------------------------------------------------------------

  /** Measures preprocessing latency in isolation: cold first call vs warm subsequent calls. */
  @Test
  void coldVsWarmPreprocessingLatency() {
    String coldQuery = REPRESENTATIVE_QUERIES.get(4); // query with FORMAT clause
    long coldStart = System.nanoTime();
    PREPROCESSOR.preprocess(coldQuery);
    long coldNanos = System.nanoTime() - coldStart;

    // Warm-up
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      for (String q : REPRESENTATIVE_QUERIES) {
        PREPROCESSOR.preprocess(q);
      }
    }

    // Measured
    long[] latencies = new long[MEASURED_ITERATIONS];
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      String query = REPRESENTATIVE_QUERIES.get(i % REPRESENTATIVE_QUERIES.size());
      long start = System.nanoTime();
      PREPROCESSOR.preprocess(query);
      latencies[i] = System.nanoTime() - start;
    }

    double warmAvg = computeAvg(latencies);
    java.util.Arrays.sort(latencies);
    long warmMedian = latencies[MEASURED_ITERATIONS / 2];

    System.out.println("=== Preprocessing Latency Benchmark ===");
    System.out.printf("Cold start:   %,d ns (%.3f ms)%n", coldNanos, coldNanos / 1e6);
    System.out.printf("Warm avg:     %,.0f ns (%.3f ms)%n", warmAvg, warmAvg / 1e6);
    System.out.printf("Warm median:  %,d ns (%.3f ms)%n", warmMedian, warmMedian / 1e6);
    System.out.println("=======================================");

    // Sanity: warm median should be reasonable (not regressed)
    assertTrue(warmMedian < coldNanos * 20,
        "Warm preprocessing median should not be wildly slower than cold start");
  }

  // -------------------------------------------------------------------------
  // Cold vs Warm: Operator Table Lookup Only
  // -------------------------------------------------------------------------

  /** Measures operator table lookup latency: cold first lookup vs warm cached lookups. */
  @Test
  void coldVsWarmOperatorLookupLatency() {
    String coldFunc = FUNCTION_NAMES.get(0);
    long coldStart = System.nanoTime();
    lookupOperator(coldFunc);
    long coldNanos = System.nanoTime() - coldStart;

    // Warm-up
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      for (String fn : FUNCTION_NAMES) {
        lookupOperator(fn);
      }
    }

    // Measured
    long[] latencies = new long[MEASURED_ITERATIONS];
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      String fn = FUNCTION_NAMES.get(i % FUNCTION_NAMES.size());
      long start = System.nanoTime();
      lookupOperator(fn);
      latencies[i] = System.nanoTime() - start;
    }

    double warmAvg = computeAvg(latencies);
    java.util.Arrays.sort(latencies);
    long warmMedian = latencies[MEASURED_ITERATIONS / 2];
    long warmP95 = latencies[(int) (MEASURED_ITERATIONS * 0.95)];

    System.out.println("=== Operator Lookup Latency Benchmark ===");
    System.out.printf("Cold start:   %,d ns (%.3f ms)%n", coldNanos, coldNanos / 1e6);
    System.out.printf("Warm avg:     %,.0f ns (%.3f ms)%n", warmAvg, warmAvg / 1e6);
    System.out.printf("Warm median:  %,d ns (%.3f ms)%n", warmMedian, warmMedian / 1e6);
    System.out.printf("Warm P95:     %,d ns (%.3f ms)%n", warmP95, warmP95 / 1e6);
    System.out.println("=========================================");

    // Sanity: warm lookups should benefit from cache
    assertTrue(warmMedian < coldNanos * 20,
        "Warm operator lookup median should not be wildly slower than cold start");
  }

  // -------------------------------------------------------------------------
  // Cold vs Warm: Parsing Only
  // -------------------------------------------------------------------------

  /** Measures SQL parsing latency in isolation (after preprocessing). */
  @Test
  void coldVsWarmParsingLatency() throws SqlParseException {
    // Pre-process all queries so we measure parsing only
    List<String> preprocessed = new ArrayList<>();
    for (String q : REPRESENTATIVE_QUERIES) {
      preprocessed.add(PREPROCESSOR.preprocess(q));
    }

    String coldQuery = preprocessed.get(0);
    long coldStart = System.nanoTime();
    SqlParser.create(coldQuery, PARSER_CONFIG).parseQuery();
    long coldNanos = System.nanoTime() - coldStart;

    // Warm-up
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
      for (String q : preprocessed) {
        SqlParser.create(q, PARSER_CONFIG).parseQuery();
      }
    }

    // Measured
    long[] latencies = new long[MEASURED_ITERATIONS];
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      String q = preprocessed.get(i % preprocessed.size());
      long start = System.nanoTime();
      SqlParser.create(q, PARSER_CONFIG).parseQuery();
      latencies[i] = System.nanoTime() - start;
    }

    double warmAvg = computeAvg(latencies);
    java.util.Arrays.sort(latencies);
    long warmMedian = latencies[MEASURED_ITERATIONS / 2];

    System.out.println("=== SQL Parsing Latency Benchmark ===");
    System.out.printf("Cold start:   %,d ns (%.3f ms)%n", coldNanos, coldNanos / 1e6);
    System.out.printf("Warm avg:     %,.0f ns (%.3f ms)%n", warmAvg, warmAvg / 1e6);
    System.out.printf("Warm median:  %,d ns (%.3f ms)%n", warmMedian, warmMedian / 1e6);
    System.out.println("=====================================");

    assertTrue(warmMedian < coldNanos * 20,
        "Warm parsing median should not be wildly slower than cold start");
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Runs the full dialect pipeline: preprocess → parse → operator lookup for all functions
   * referenced in the query.
   */
  private void runPipeline(String query) throws SqlParseException {
    // Step 1: Preprocess
    String preprocessed = PREPROCESSOR.preprocess(query);

    // Step 2: Parse
    SqlParser.create(preprocessed, PARSER_CONFIG).parseQuery();

    // Step 3: Operator table lookups for representative functions
    for (String fn : FUNCTION_NAMES) {
      lookupOperator(fn);
    }
  }

  private void lookupOperator(String functionName) {
    List<SqlOperator> result = new ArrayList<>();
    SqlIdentifier id =
        new SqlIdentifier(
            functionName.toUpperCase(Locale.ROOT), SqlParserPos.ZERO);
    OPERATOR_TABLE.lookupOperatorOverloads(
        id, null, SqlSyntax.FUNCTION, result, SqlNameMatchers.liberal());
  }

  private double computeAvg(long[] values) {
    long sum = 0;
    for (long v : values) {
      sum += v;
    }
    return (double) sum / values.length;
  }
}
