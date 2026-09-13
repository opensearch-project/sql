/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
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
                    + " | stats count() as pattern_count, max(content) as sample_logs"
                    + "   by patterns_field"
                    + " | sort - pattern_count"
                    + " | fields patterns_field, pattern_count, sample_logs",
                TEST_INDEX_HDFS_LOGS));
    verifySchemaInOrder(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("sample_logs", "string"));
    // Each of the four BRAIN patterns covers exactly two documents (count 2, shard-invariant).
    // max(content) picks the lexicographically-largest of a pattern's two source lines, which is
    // order-insensitive and therefore stable across shards and routes, so the sample is exact
    // rather than an arbitrary take(content, 1). Row order is order-insensitive here (all counts
    // tie at 2).
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>",
            2,
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864"),
        rows(
            "PacketResponder failed <*> blk_<*>",
            2,
            "PacketResponder failed for blk_6996194389878584395"),
        rows(
            "Verification succeeded <*> blk_<*>",
            2,
            "Verification succeeded for blk_6996194389878584395"),
        rows(
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>",
            2,
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                + " blk_2096692261399680562"));
  }
}
