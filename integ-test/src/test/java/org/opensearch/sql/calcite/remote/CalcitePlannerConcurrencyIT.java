/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for {@code UnifiedQueryPlanner} state isolation under concurrent load.
 *
 * <p>The planner's post-analysis pipeline (extensions registered via {@code
 * LanguageSpec.postAnalysisRules}) uses Calcite {@code RelShuttle} subclasses. {@code
 * RelShuttleImpl} inherits a non-thread-safe {@code ArrayDeque<RelNode>} stack used by {@code
 * visitChild}'s push/pop. Any extension that returns the same shuttle instance across {@code
 * plan()} calls is unsafe under concurrent load: cluster threads call {@code plan()} simultaneously
 * and their push/pop on the shared stack interleave, leaving residual entries that surface on a
 * subsequent traversal as {@code NoSuchElementException} at {@code RelShuttleImpl.visitChild} line
 * 67 (the {@code stack.pop()} in the {@code finally} block).
 *
 * <p>The test methods here fire many queries through a thread pool to exercise concurrent {@code
 * plan()} invocations. New planner-level concurrency / state-isolation regressions belong in this
 * class.
 *
 * <p>Run via:
 *
 * <pre>{@code
 * ./gradlew :integ-test:integTestRemote \
 *   -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9300 \
 *   -Dtests.clustername=runTask \
 *   -Dtests.analytics.force_routing=true \
 *   -Dtests.analytics.parquet_indices=true \
 *   --tests org.opensearch.sql.calcite.remote.CalcitePlannerConcurrencyIT
 * }</pre>
 */
public class CalcitePlannerConcurrencyIT extends PPLIntegTestCase {

  /** Concurrency level — matches the rough parallelism of a dashboard field-stats panel. */
  private static final int PARALLELISM = 8;

  /** Total queries fired per test. */
  private static final int QUERIES = 80;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // DATE_FORMATS has many datetime columns of different formats/precisions. With
    // -Dtests.analytics.parquet_indices=true the helper provisions it as a parquet-backed
    // composite index — required for analytics-engine routing.
    loadIndex(Index.DATE_FORMATS);
  }

  @Test
  public void testConcurrentStatsDistinctCountOverDatetime() throws Exception {
    String[] fields = {
      "epoch_millis", "epoch_second", "date_optional_time", "strict_date_optional_time"
    };
    List<String> queries = new ArrayList<>(QUERIES);
    for (int i = 0; i < QUERIES; i++) {
      String field = fields[i % fields.length];
      queries.add(
          String.format(
              "source=%s | stats count() as field_count, distinct_count(%s) as distinct_count",
              TEST_INDEX_DATE_FORMATS, field));
    }
    executeConcurrent(queries);
  }

  @Test
  public void testConcurrentMixedDatetimePlans() throws Exception {
    // Mix three plan shapes: stats+distinct_count, plain field projection (datetime cast), and
    // stats by a different field. Different plan shapes push the planner's post-analysis shuttles
    // through different visitChild call counts — amplifying any cross-query stack pollution.
    List<String> shapes =
        List.of(
            "source=%s | stats count() as field_count, distinct_count(epoch_millis) as"
                + " distinct_count",
            "source=%s | fields epoch_millis, epoch_second, date_optional_time",
            "source=%s | stats count() as field_count, distinct_count(epoch_second) as"
                + " distinct_count by date_optional_time");
    List<String> queries = new ArrayList<>(QUERIES);
    for (int i = 0; i < QUERIES; i++) {
      queries.add(String.format(shapes.get(i % shapes.size()), TEST_INDEX_DATE_FORMATS));
    }
    executeConcurrent(queries);
  }

  /**
   * Fire all queries through a fixed-size thread pool. Asserts every query completes without
   * exception. With a shuttle-state-leak bug present this triggers {@code NoSuchElementException}
   * on at least one task once the stack interleaving corrupts state.
   */
  private void executeConcurrent(List<String> queries) throws Exception {
    var executor = Executors.newFixedThreadPool(PARALLELISM);
    try {
      List<CompletableFuture<Void>> futures = new ArrayList<>(queries.size());
      AtomicInteger failures = new AtomicInteger();
      List<Throwable> errors = new ArrayList<>();
      for (String query : queries) {
        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    executeQuery(query);
                  } catch (Exception e) {
                    failures.incrementAndGet();
                    synchronized (errors) {
                      errors.add(e);
                    }
                  }
                },
                executor));
      }
      for (CompletableFuture<Void> f : futures) {
        try {
          f.get(60, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          failures.incrementAndGet();
          synchronized (errors) {
            errors.add(e.getCause());
          }
        }
      }
      if (failures.get() > 0) {
        StringBuilder msg = new StringBuilder();
        msg.append(failures.get()).append("/").append(queries.size()).append(" queries failed:");
        synchronized (errors) {
          for (int i = 0; i < Math.min(3, errors.size()); i++) {
            msg.append("\n  - ").append(errors.get(i));
          }
        }
        throw new AssertionError(msg.toString());
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
