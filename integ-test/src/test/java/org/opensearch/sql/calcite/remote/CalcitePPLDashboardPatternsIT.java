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

/** Pins the BRAIN-label pattern panel query shape used by OpenSearch Dashboards. */
public class CalcitePPLDashboardPatternsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.HDFS_LOGS);
  }

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
