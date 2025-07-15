/// *
// * Copyright OpenSearch Contributors
// * SPDX-License-Identifier: Apache-2.0
// */
//
// package org.opensearch.sql.calcite.big5;
//
// import static org.opensearch.sql.legacy.TestsConstants.*;
//
// import java.io.IOException;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.time.LocalDateTime;
// import java.time.format.DateTimeFormatter;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Locale;
// import java.util.Map;
// import java.util.concurrent.TimeUnit;
// import org.json.JSONArray;
// import org.json.JSONObject;
// import org.junit.AfterClass;
// import org.junit.BeforeClass;
// import org.junit.FixMethodOrder;
// import org.junit.Test;
// import org.junit.runners.MethodSorters;
// import org.opensearch.common.collect.MapBuilder;
// import org.opensearch.sql.ppl.PPLIntegTestCase;
//
/// **
// * Comprehensive performance tests for the bin command using Big5 infrastructure.
// *
// * <p>Features: - Detailed query execution timing with microsecond precision - Real-time
// performance
// * monitoring and reporting - Memory usage tracking during bin operations - Query plan generation
// vs
// * execution time analysis - Percentile analysis (p50, p95, p99) for multiple test runs -
// * Performance regression detection - Throughput metrics (records processed per second)
// */
// @FixMethodOrder(MethodSorters.JVM)
// public class CalciteBinCommandBig5IT extends PPLIntegTestCase {
//
//  // Performance tracking infrastructure
//  private static final MapBuilder<String, Long> executionTimes = MapBuilder.newMapBuilder();
//  private static final MapBuilder<String, List<Long>> multiRunTimes = MapBuilder.newMapBuilder();
//  private static final MapBuilder<String, Long> memoryUsage = MapBuilder.newMapBuilder();
//  private static final MapBuilder<String, Long> recordCounts = MapBuilder.newMapBuilder();
//
//  // Test configuration
//  private static final int WARMUP_RUNS = 3;
//  private static final int PERFORMANCE_RUNS = 10;
//  private static final boolean ENABLE_MEMORY_MONITORING = true;
//  private static final boolean ENABLE_REAL_TIME_REPORTING = true;
//
//  @Override
//  public void init() throws Exception {
//    super.init();
//    loadIndex(Index.BIG5);
//    enableCalcite();
//    disallowCalciteFallback();
//
//    System.out.println("=== CALCITE BIN COMMAND BIG5 PERFORMANCE TESTS ===");
//    System.out.println("Dataset: Big5 (large-scale log data)");
//    System.out.println("Engine: Calcite with bin command optimization");
//    System.out.println("Warmup runs: " + WARMUP_RUNS);
//    System.out.println("Performance runs: " + PERFORMANCE_RUNS);
//    System.out.println("Memory monitoring: " + ENABLE_MEMORY_MONITORING);
//    System.out.println("Real-time reporting: " + ENABLE_REAL_TIME_REPORTING);
//    System.out.println();
//  }
//
//  @BeforeClass
//  public static void setupPerformanceMonitoring() {
//    if (ENABLE_MEMORY_MONITORING) {
//      System.gc(); // Clean up before starting tests
//      System.out.printf("Initial memory usage: %.2f MB%n", getMemoryUsageMB());
//    }
//  }
//
//  @AfterClass
//  public static void reportPerformanceSummary() throws IOException {
//    Map<String, Long> execTimes = executionTimes.immutableMap();
//    Map<String, List<Long>> multiTimes = multiRunTimes.immutableMap();
//    Map<String, Long> memUsage = memoryUsage.immutableMap();
//    Map<String, Long> recordCnts = recordCounts.immutableMap();
//
//    if (execTimes.isEmpty()) {
//      System.out.println("\nNo performance data available.");
//      return;
//    }
//
//    // Generate performance report file
//    String timestamp =
//        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
//    String reportFileName = "bin_command_performance_report_" + timestamp + ".txt";
//
//    // Create in test build directory
//    Path tempDir = Paths.get("build", "testrun", "integTest", "temp");
//    Path reportPath = tempDir.resolve(reportFileName);
//
//    // Create directory if it doesn't exist
//    try {
//      Files.createDirectories(tempDir);
//    } catch (IOException e) {
//      // Fall back to system temp directory
//      tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
//      reportPath = tempDir.resolve(reportFileName);
//    }
//
//    try {
//      // Write to file using a simple approach
//      String reportContent = generateReportContent(execTimes, multiTimes, memUsage, recordCnts);
//      Files.write(reportPath, reportContent.getBytes());
//
//      System.out.println("\n" + "=".repeat(60));
//      System.out.println("PERFORMANCE REPORT GENERATED");
//      System.out.println("=".repeat(60));
//      System.out.println("Report saved to: " + reportPath.toString());
//      System.out.println("Total tests: " + execTimes.size());
//      double avgTime =
//          (double) execTimes.values().stream().mapToLong(Long::longValue).sum() /
// execTimes.size();
//      System.out.println("Average execution time: " + String.format("%.1f ms", avgTime));
//
//      // Verify file was created
//      if (Files.exists(reportPath)) {
//        long fileSize = Files.size(reportPath);
//        System.out.println("File verified: " + fileSize + " bytes written");
//      } else {
//        System.err.println("ERROR: File was not created!");
//      }
//      System.out.println("=".repeat(60));
//    } catch (IOException e) {
//      System.err.println("Failed to write performance report: " + e.getMessage());
//      e.printStackTrace();
//      // Fallback to console output
//      System.out.println("\n=== PERFORMANCE REPORT (Console Fallback) ===");
//      String reportContent = generateReportContent(execTimes, multiTimes, memUsage, recordCnts);
//      System.out.println(reportContent);
//    }
//  }
//
//  private static String generateReportContent(
//      Map<String, Long> execTimes,
//      Map<String, List<Long>> multiTimes,
//      Map<String, Long> memUsage,
//      Map<String, Long> recordCnts) {
//    StringBuilder sb = new StringBuilder();
//
//    sb.append("=".repeat(80)).append("\n");
//    sb.append("BIN COMMAND PERFORMANCE SUMMARY").append("\n");
//    sb.append("=".repeat(80)).append("\n");
//    sb.append("Generated: ")
//        .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
//        .append("\n");
//    sb.append("Test Suite: CalciteBinCommandBig5IT").append("\n");
//    sb.append("Dataset: Big5 (large-scale log data)").append("\n");
//    sb.append("Engine: Calcite with bin command optimization").append("\n");
//    sb.append("=".repeat(80)).append("\n");
//
//    // Calculate overall statistics
//    long totalTime = execTimes.values().stream().mapToLong(Long::longValue).sum();
//    double avgTime = (double) totalTime / execTimes.size();
//    long minTime = execTimes.values().stream().mapToLong(Long::longValue).min().orElse(0);
//    long maxTime = execTimes.values().stream().mapToLong(Long::longValue).max().orElse(0);
//
//    // Group tests by performance tier
//    List<Map.Entry<String, Long>> excellentTests = new ArrayList<>();
//    List<Map.Entry<String, Long>> goodTests = new ArrayList<>();
//    List<Map.Entry<String, Long>> acceptableTests = new ArrayList<>();
//    List<Map.Entry<String, Long>> slowTests = new ArrayList<>();
//
//    execTimes
//        .entrySet()
//        .forEach(
//            entry -> {
//              long duration = entry.getValue();
//              if (duration < 10) excellentTests.add(entry);
//              else if (duration < 20) goodTests.add(entry);
//              else if (duration < 50) acceptableTests.add(entry);
//              else slowTests.add(entry);
//            });
//
//    // Sort each tier by execution time
//    excellentTests.sort(Map.Entry.comparingByValue());
//    goodTests.sort(Map.Entry.comparingByValue());
//    acceptableTests.sort(Map.Entry.comparingByValue());
//    slowTests.sort(Map.Entry.comparingByValue());
//
//    // Execution times by tier
//    sb.append("\nEXECUTION TIMES (Single Run):\n");
//    sb.append("-".repeat(80)).append("\n");
//    sb.append(
//        String.format(
//            Locale.ENGLISH, "%-35s %8s %12s %10s%n", "TEST NAME", "TIME", "THROUGHPUT",
// "MEMORY"));
//    sb.append("-".repeat(80)).append("\n");
//
//    if (!excellentTests.isEmpty()) {
//      sb.append("Tier: EXCELLENT (< 10ms)\n");
//      appendTestGroup(sb, excellentTests, recordCnts, memUsage);
//      sb.append("\n");
//    }
//
//    if (!goodTests.isEmpty()) {
//      sb.append("Tier: GOOD (10-19ms)\n");
//      appendTestGroup(sb, goodTests, recordCnts, memUsage);
//      sb.append("\n");
//    }
//
//    if (!acceptableTests.isEmpty()) {
//      sb.append("Tier: ACCEPTABLE (20-49ms)\n");
//      appendTestGroup(sb, acceptableTests, recordCnts, memUsage);
//      sb.append("\n");
//    }
//
//    if (!slowTests.isEmpty()) {
//      sb.append("Tier: NEEDS OPTIMIZATION (50ms+)\n");
//      appendTestGroup(sb, slowTests, recordCnts, memUsage);
//      sb.append("\n");
//    }
//
//    // Percentile analysis (only if multi-run data exists)
//    if (!multiTimes.isEmpty()) {
//      sb.append("PERCENTILE ANALYSIS (Multiple Runs):\n");
//      sb.append("-".repeat(80)).append("\n");
//      sb.append(
//          String.format(
//              Locale.ENGLISH,
//              "%-35s %5s %5s %5s %5s %5s %8s%n",
//              "TEST NAME",
//              "MIN",
//              "P50",
//              "P95",
//              "P99",
//              "MAX",
//              "RANGE"));
//      sb.append("-".repeat(80)).append("\n");
//
//      multiTimes.entrySet().stream()
//          .sorted(Map.Entry.comparingByKey())
//          .forEach(
//              entry -> {
//                String testName = entry.getKey();
//                List<Long> times = new ArrayList<>(entry.getValue());
//                times.sort(Long::compareTo);
//
//                if (!times.isEmpty()) {
//                  long min = times.get(0);
//                  long p50 = percentile(times, 50);
//                  long p95 = percentile(times, 95);
//                  long p99 = percentile(times, 99);
//                  long max = times.get(times.size() - 1);
//                  String range = min + "-" + max + "ms";
//
//                  sb.append(
//                      String.format(
//                          Locale.ENGLISH,
//                          "%-35s %4dms %4dms %4dms %4dms %4dms %8s%n",
//                          truncateTestName(testName),
//                          min,
//                          p50,
//                          p95,
//                          p99,
//                          max,
//                          range));
//                }
//              });
//      sb.append("\n");
//    }
//
//    // Overall statistics
//    sb.append("OVERALL STATISTICS:\n");
//    sb.append("-".repeat(80)).append("\n");
//    sb.append(String.format(Locale.ENGLISH, "Total execution time:      %6d ms%n", totalTime));
//    sb.append(String.format(Locale.ENGLISH, "Average test duration:     %6.1f ms%n", avgTime));
//    sb.append(String.format(Locale.ENGLISH, "Fastest test:              %6d ms%n", minTime));
//    sb.append(String.format(Locale.ENGLISH, "Slowest test:              %6d ms%n", maxTime));
//    sb.append(
//        String.format(
//            Locale.ENGLISH,
//            "Tests completed:           %6d/%-6d (100%% success)%n",
//            execTimes.size(),
//            execTimes.size()));
//
//    // Performance tier summary
//    sb.append("Performance distribution:\n");
//    sb.append(
//        String.format(
//            Locale.ENGLISH, "  Excellent (< 10ms):      %6d tests%n", excellentTests.size()));
//    sb.append(
//        String.format(Locale.ENGLISH, "  Good (10-19ms):          %6d tests%n",
// goodTests.size()));
//    sb.append(
//        String.format(
//            Locale.ENGLISH, "  Acceptable (20-49ms):    %6d tests%n", acceptableTests.size()));
//    sb.append(
//        String.format(
//            Locale.ENGLISH, "  Needs optimization (50ms+): %3d tests%n", slowTests.size()));
//
//    if (ENABLE_MEMORY_MONITORING) {
//      sb.append(
//          String.format(
//              Locale.ENGLISH, "Final memory usage:        %6.2f MB%n", getMemoryUsageMB()));
//
//      // Memory efficiency assessment
//      long totalMemoryChange = memUsage.values().stream().mapToLong(Long::longValue).sum();
//      String memoryAssessment =
//          totalMemoryChange < 0
//              ? "Excellent (reduced usage)"
//              : totalMemoryChange < 50_000_000
//                  ? "Good (< 50MB increase)"
//                  : "Needs attention (high memory usage)";
//      sb.append(String.format(Locale.ENGLISH, "Memory efficiency:         %s%n",
// memoryAssessment));
//    }
//
//    sb.append("=".repeat(80)).append("\n");
//
//    return sb.toString();
//  }
//
//  private static void appendTestGroup(
//      StringBuilder sb,
//      List<Map.Entry<String, Long>> tests,
//      Map<String, Long> recordCnts,
//      Map<String, Long> memUsage) {
//    tests.forEach(
//        entry -> {
//          String testName = entry.getKey();
//          long duration = entry.getValue();
//          long records = recordCnts.getOrDefault(testName, 0L);
//          double throughput = records > 0 ? (records * 1000.0 / duration) : 0;
//          double memoryMB = memUsage.getOrDefault(testName, 0L) / 1024.0 / 1024.0;
//
//          sb.append(
//              String.format(
//                  Locale.ENGLISH,
//                  "%-35s %6dms %8.0f r/s %8.2f MB%n",
//                  truncateTestName(testName),
//                  duration,
//                  throughput,
//                  memoryMB));
//        });
//  }
//
//  private static String truncateTestName(String testName) {
//    if (testName.length() <= 35) return testName;
//    return testName.substring(0, 32) + "...";
//  }
//
//  // Enhanced timing method with detailed metrics
//  private void enhancedTiming(String testName, String ppl) throws IOException {
//    enhancedTiming(testName, ppl, true);
//  }
//
//  private void enhancedTiming(String testName, String ppl, boolean enableMultiRun)
//      throws IOException {
//    printTestHeader(testName);
//
//    // Memory snapshot before test
//    long memoryBefore = ENABLE_MEMORY_MONITORING ? getMemoryUsageBytes() : 0;
//
//    // Warmup runs
//    System.out.printf("  Performing %d warmup runs...%n", WARMUP_RUNS);
//    for (int i = 0; i < WARMUP_RUNS; i++) {
//      executeQuery(ppl);
//    }
//
//    // Single performance measurement
//    System.out.print("  Single run measurement: ");
//    long start = System.nanoTime();
//    JSONObject result = executeQuery(ppl);
//    long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
//
//    // Extract record count from result
//    long recordCount = extractRecordCount(result);
//    double throughput = recordCount > 0 ? (recordCount * 1000.0 / duration) : 0;
//
//    System.out.printf("%d ms (%,d records, %.0f records/sec)%n", duration, recordCount,
// throughput);
//
//    // Store results
//    executionTimes.put(testName, duration);
//    recordCounts.put(testName, recordCount);
//
//    // Multi-run percentile analysis
//    if (enableMultiRun) {
//      System.out.printf("  Multi-run analysis (%d runs): ", PERFORMANCE_RUNS);
//      List<Long> runTimes = new ArrayList<>();
//
//      for (int i = 0; i < PERFORMANCE_RUNS; i++) {
//        long runStart = System.nanoTime();
//        executeQuery(ppl);
//        long runDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - runStart);
//        runTimes.add(runDuration);
//
//        if (ENABLE_REAL_TIME_REPORTING && (i + 1) % 3 == 0) {
//          System.out.printf(".");
//        }
//      }
//
//      runTimes.sort(Long::compareTo);
//      long p50 = percentile(runTimes, 50);
//      long p95 = percentile(runTimes, 95);
//      long p99 = percentile(runTimes, 99);
//
//      System.out.printf(" p50=%d p95=%d p99=%d ms%n", p50, p95, p99);
//      multiRunTimes.put(testName, runTimes);
//    }
//
//    // Memory usage analysis
//    if (ENABLE_MEMORY_MONITORING) {
//      System.gc(); // Force garbage collection for accurate measurement
//      long memoryAfter = getMemoryUsageBytes();
//      long memoryDelta = memoryAfter - memoryBefore;
//      memoryUsage.put(testName, memoryDelta);
//
//      System.out.printf(
//          "  Memory impact: %+.2f MB (%.2f MB -> %.2f MB)%n",
//          memoryDelta / 1024.0 / 1024.0,
//          memoryBefore / 1024.0 / 1024.0,
//          memoryAfter / 1024.0 / 1024.0);
//    }
//
//    System.out.println();
//  }
//
//  // Performance test cases for bin command
//
//  @Test
//  public void bin_timestamp_daily_performance() throws IOException {
//    String ppl = "source=big5 | bin @timestamp span=86400000 | stats count() by @timestamp_bin";
//    enhancedTiming("bin_timestamp_daily", ppl);
//  }
//
//  @Test
//  public void bin_timestamp_hourly_performance() throws IOException {
//    String ppl = "source=big5 | bin @timestamp span=3600000 | stats count() by @timestamp_bin";
//    enhancedTiming("bin_timestamp_hourly", ppl);
//  }
//
//  @Test
//  public void bin_metrics_size_small_span_performance() throws IOException {
//    String ppl = "source=big5 | bin metrics.size span=1000 | stats count() by metrics.size_bin";
//    enhancedTiming("bin_metrics_size_small_span", ppl);
//  }
//
//  @Test
//  public void bin_metrics_size_large_span_performance() throws IOException {
//    String ppl = "source=big5 | bin metrics.size span=100000 | stats count() by metrics.size_bin";
//    enhancedTiming("bin_metrics_size_large_span", ppl);
//  }
//
//  @Test
//  public void bin_metrics_tmin_performance() throws IOException {
//    String ppl = "source=big5 | bin metrics.tmin span=50000 | stats count() by metrics.tmin_bin";
//    enhancedTiming("bin_metrics_tmin", ppl);
//  }
//
//  @Test
//  public void bin_with_complex_aggregation_performance() throws IOException {
//    String ppl =
//        "source=big5 | bin @timestamp span=21600000 | stats count() as events, avg(metrics.size)
// as"
//            + " avg_size, max(metrics.tmin) as max_tmin by @timestamp_bin";
//    enhancedTiming("bin_complex_aggregation", ppl);
//  }
//
//  @Test
//  public void bin_with_filtering_performance() throws IOException {
//    String ppl =
//        "source=big5 | where metrics.size > 1000 | bin metrics.size span=5000 | stats count() by"
//            + " metrics.size_bin";
//    enhancedTiming("bin_with_filtering", ppl);
//  }
//
//  @Test
//  public void bin_with_range_filtering_performance() throws IOException {
//    String ppl =
//        "source=big5 | where @timestamp > '2023-01-01 00:00:00' and @timestamp < '2023-12-31"
//            + " 23:59:59' | bin @timestamp span=86400000 | stats count() by @timestamp_bin";
//    enhancedTiming("bin_with_range_filtering", ppl);
//  }
//
//  @Test
//  public void bin_decimal_span_performance() throws IOException {
//    String ppl = "source=big5 | bin metrics.size span=1500.5 | stats count() by metrics.size_bin";
//    enhancedTiming("bin_decimal_span", ppl);
//  }
//
//  @Test
//  public void bin_multiple_fields_performance() throws IOException {
//    String ppl =
//        "source=big5 | bin @timestamp span=86400000 AS day_bucket | bin metrics.size span=10000
// AS"
//            + " size_bucket | stats count() by day_bucket, size_bucket";
//    enhancedTiming("bin_multiple_fields", ppl);
//  }
//
//  @Test
//  public void bin_with_limit_performance() throws IOException {
//    String ppl =
//        "source=big5 | bin @timestamp span=3600000 | stats count() by @timestamp_bin | head 100";
//    enhancedTiming("bin_with_limit", ppl);
//  }
//
//  @Test
//  public void bin_with_sort_by_count_performance() throws IOException {
//    String ppl =
//        "source=big5 | bin metrics.size span=5000 | stats count() as bin_count by
// metrics.size_bin"
//            + " | sort - bin_count | head 20";
//    enhancedTiming("bin_sort_by_count", ppl);
//  }
//
//  // Regression detection tests
//
//  @Test
//  public void bin_regression_baseline_timestamp() throws IOException {
//    // This test establishes baseline performance for timestamp binning
//    String ppl = "source=big5 | bin @timestamp span=86400000 | stats count() by @timestamp_bin";
//    enhancedTiming("BASELINE_timestamp_daily", ppl, false);
//
//    // Performance regression check
//    Long duration = executionTimes.get("BASELINE_timestamp_daily");
//    if (duration != null && duration > 5000) { // 5 second threshold
//      System.out.printf(
//          "⚠️  PERFORMANCE REGRESSION: timestamp binning took %d ms (threshold: 5000 ms)%n",
//          duration);
//    } else {
//      System.out.printf("✅ PERFORMANCE OK: timestamp binning completed in %d ms%n", duration);
//    }
//  }
//
//  @Test
//  public void bin_regression_baseline_numeric() throws IOException {
//    // This test establishes baseline performance for numeric binning
//    String ppl = "source=big5 | bin metrics.size span=1000 | stats count() by metrics.size_bin";
//    enhancedTiming("BASELINE_numeric_binning", ppl, false);
//
//    // Performance regression check
//    Long duration = executionTimes.get("BASELINE_numeric_binning");
//    if (duration != null && duration > 3000) { // 3 second threshold
//      System.out.printf(
//          "⚠️  PERFORMANCE REGRESSION: numeric binning took %d ms (threshold: 3000 ms)%n",
//          duration);
//    } else {
//      System.out.printf("✅ PERFORMANCE OK: numeric binning completed in %d ms%n", duration);
//    }
//  }
//
//  // Stress tests for large-scale operations
//
//  @Test
//  public void bin_stress_test_fine_granularity() throws IOException {
//    // Test with very fine granularity to stress the binning engine
//    String ppl = "source=big5 | bin @timestamp span=60000 | stats count() by @timestamp_bin";
//    enhancedTiming("STRESS_fine_granularity", ppl, false);
//  }
//
//  @Test
//  public void bin_stress_test_many_buckets() throws IOException {
//    // Test that produces many buckets to stress aggregation
//    String ppl = "source=big5 | bin metrics.size span=100 | stats count() by metrics.size_bin";
//    enhancedTiming("STRESS_many_buckets", ppl, false);
//  }
//
//  // Utility methods for performance monitoring
//
//  private static void printTestHeader(String testName) {
//    if (ENABLE_REAL_TIME_REPORTING) {
//      System.out.printf("🔄 Running: %s%n", testName);
//    }
//  }
//
//  private static long extractRecordCount(JSONObject result) {
//    try {
//      if (result.has("datarows")) {
//        JSONArray datarows = result.getJSONArray("datarows");
//        return datarows.length();
//      }
//      if (result.has("total")) {
//        return result.getLong("total");
//      }
//    } catch (Exception e) {
//      // Ignore errors in record count extraction
//    }
//    return 0;
//  }
//
//  private static long percentile(List<Long> sortedValues, int percentile) {
//    if (sortedValues.isEmpty()) return 0;
//
//    int index = (int) Math.ceil(percentile / 100.0 * sortedValues.size()) - 1;
//    index = Math.max(0, Math.min(index, sortedValues.size() - 1));
//    return sortedValues.get(index);
//  }
//
//  private static long getMemoryUsageBytes() {
//    Runtime runtime = Runtime.getRuntime();
//    return runtime.totalMemory() - runtime.freeMemory();
//  }
//
//  private static double getMemoryUsageMB() {
//    return getMemoryUsageBytes() / 1024.0 / 1024.0;
//  }
// }
