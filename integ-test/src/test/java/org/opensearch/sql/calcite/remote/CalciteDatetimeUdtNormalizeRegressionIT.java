/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertNotNull;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Regression IT for the {@code DatetimeUdtNormalizeRule} / {@code DatetimeOutputCastRule}
 * singleton-stack-corruption bug.
 *
 * <p>Both rules extend Calcite's {@code RelHomogeneousShuttle}, which inherits a stateful {@code
 * Deque<RelNode>} stack from {@code RelShuttleImpl}. Earlier code returned the same {@code
 * INSTANCE} of each rule from {@code DatetimeExtension.postAnalysisRules()} on every {@code
 * UnifiedQueryPlanner.plan()} call. If any traversal ever finished with an unbalanced stack,
 * residual entries persisted to the next query and the next {@code visitChild()} popped a stale or
 * empty stack — surfacing as {@code NoSuchElementException} at {@code RelShuttleImpl.visitChild}
 * line 67.
 *
 * <p>The failure was reported as intermittent on dashboards issuing field-statistics queries of the
 * shape {@code stats count() as field_count, distinct_count(field)} against parquet-backed indices.
 * This IT runs the same query shape repeatedly against a parquet-backed (composite) index with
 * multiple datetime columns to exercise the analytics-engine route end-to-end.
 *
 * <p>Run via:
 *
 * <pre>{@code
 * ./gradlew :integ-test:integTestRemote \
 *   -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9300 \
 *   -Dtests.clustername=runTask \
 *   -Dtests.analytics.force_routing=true \
 *   -Dtests.analytics.parquet_indices=true \
 *   --tests org.opensearch.sql.calcite.remote.CalciteDatetimeUdtNormalizeRegressionIT
 * }</pre>
 */
public class CalciteDatetimeUdtNormalizeRegressionIT extends PPLIntegTestCase {

  /** Number of repetitions per test. Singleton bug surfaces order-dependent across plan() calls. */
  private static final int ITERATIONS = 20;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // DATE_FORMATS index has many date columns of different formats / precisions, which is what
    // dashboard field-statistics panels iterate over. Loaded through the helper so that with
    // -Dtests.analytics.parquet_indices=true it gets provisioned as a parquet-backed composite
    // index — required for analytics-engine routing.
    loadIndex(Index.DATE_FORMATS);
  }

  @Test
  public void testSequentialStatsDistinctCountOverDatetime() throws IOException {
    // Bug repro: each iteration runs a stats + distinct_count over a datetime field. With the
    // singleton rule, residual entries on the shuttle's internal stack from the previous plan()
    // would cause NoSuchElementException on a subsequent traversal. With fresh instances per
    // plan(), the stack is always empty at entry and the iterations all succeed.
    String query =
        String.format(
            "source=%s | stats count() as field_count, distinct_count(epoch_millis) as"
                + " distinct_count",
            TEST_INDEX_DATE_FORMATS);
    for (int i = 0; i < ITERATIONS; i++) {
      JSONObject result = executeQuery(query);
      // Sanity: response was returned without a cluster-side exception.
      assertNotNull("iteration " + i + " produced no result", result);
    }
  }

  @Test
  public void testInterleavedStatsAndDatetimeProjection() throws IOException {
    // Bug repro variant: interleave stats+distinct_count with a plain datetime projection that
    // exercises the DatetimeOutputCastRule. Different plan shapes per iteration push the rule
    // through different visitChild paths and surface stack desync faster.
    for (int i = 0; i < ITERATIONS; i++) {
      executeQuery(
          String.format(
              "source=%s | stats count() as field_count, distinct_count(epoch_millis) as"
                  + " distinct_count",
              TEST_INDEX_DATE_FORMATS));
      executeQuery(
          String.format(
              "source=%s | fields epoch_millis, epoch_second, date_optional_time",
              TEST_INDEX_DATE_FORMATS));
      executeQuery(
          String.format(
              "source=%s | stats count() as field_count, distinct_count(epoch_second) as"
                  + " distinct_count",
              TEST_INDEX_DATE_FORMATS));
    }
  }

  @Test
  public void testDistinctCountOverMultipleDatetimeFields() throws IOException {
    // Bug repro variant: iterate distinct_count over different datetime fields in sequence —
    // mirrors the dashboard field-statistics tab that probes every field in the index.
    String[] datetimeFields = {
      "epoch_millis", "epoch_second", "date_optional_time", "strict_date_optional_time"
    };
    for (int i = 0; i < ITERATIONS; i++) {
      String field = datetimeFields[i % datetimeFields.length];
      executeQuery(
          String.format(
              "source=%s | stats count() as field_count, distinct_count(%s) as distinct_count",
              TEST_INDEX_DATE_FORMATS, field));
    }
  }
}
