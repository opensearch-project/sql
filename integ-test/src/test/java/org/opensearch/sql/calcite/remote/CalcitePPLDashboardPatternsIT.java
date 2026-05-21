/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Pins the exact PPL query shape OpenSearch Dashboards uses to render BRAIN-pattern
 * panels: {@code patterns ... method=BRAIN mode=label} followed by
 * {@code stats count(), take(message, 1) by patterns_field | sort -pattern_count |
 * fields patterns_field, pattern_count, sample_logs}.
 *
 * <p>The combination exercises three pieces that all have to be wired through the
 * analytics-engine route together:
 *
 * <ul>
 *   <li>{@code INTERNAL_PATTERN} as a window function (label mode emits one
 *       matched wildcard pattern per row, broadcast across the partition).</li>
 *   <li>{@code take(field, n)} aggregate to capture a representative sample log
 *       per discovered pattern group.</li>
 *   <li>{@code count()} aggregate + {@code sort} on the result.</li>
 * </ul>
 *
 * <p>Schema-only assertions: BRAIN's clustering depends on a corpus large enough
 * that the default heuristics fire, so per-row pattern strings are sensitive to
 * dataset version. The schema check guarantees the query plans, executes, and
 * returns the dashboard's three expected columns in the right order.
 *
 * @opensearch.internal
 */
public class CalcitePPLDashboardPatternsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.HDFS_LOGS);
  }

  /**
   * Mirrors the canonical Dashboards BRAIN-label pattern panel query. Unfiltered
   * variant — pins the end-to-end plan compiles and returns the dashboard's
   * three-column shape (matched wildcard pattern + occurrence count + sample log).
   */
  @Test
  public void testDashboardBrainLabelStatsByPatternsField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s"
                    + " | patterns content method=BRAIN mode=label"
                    + "   max_sample_count=5 variable_count_threshold=5"
                    + "   frequency_threshold_percentage=0.2"
                    + " | stats count() as pattern_count, take(content, 1) as sample_logs"
                    + "   by patterns_field"
                    + " | sort - pattern_count"
                    + " | fields patterns_field, pattern_count, sample_logs",
                TEST_INDEX_HDFS_LOGS));
    verifySchemaInOrder(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("sample_logs", "array"));
  }
}
