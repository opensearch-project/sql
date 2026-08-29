/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
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
    // Every pattern's count is 2 (shard-invariant) but take(content, 1) samples one arbitrary
    // matching document, which has no stable cross-shard tiebreaker (and all counts tie, so the
    // sort - pattern_count row order is also unstable). Assert each pattern's count and that its
    // single sampled log is one of the two documents that carry the pattern.
    Map<String, List<String>> universe = new HashMap<>();
    universe.put(
        "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size <*>",
        Arrays.asList(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
                + " blk_-3249711809227781266 size 67108864"));
    universe.put(
        "PacketResponder failed <*> blk_<*>",
        Arrays.asList(
            "PacketResponder failed for blk_6996194389878584395",
            "PacketResponder failed for blk_-1547954353065580372"));
    universe.put(
        "Verification succeeded <*> blk_<*>",
        Arrays.asList(
            "Verification succeeded for blk_-1547954353065580372",
            "Verification succeeded for blk_6996194389878584395"));
    universe.put(
        "<*> NameSystem.allocateBlock:"
            + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>",
        Arrays.asList(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                + " blk_-6620182933895093708",
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                + " blk_2096692261399680562"));

    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(4, datarows.length());
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      String pattern = row.getString(0);
      assertTrue("unexpected pattern: " + pattern, universe.containsKey(pattern));
      assertEquals("count for " + pattern, 2L, row.getLong(1));
      JSONArray samples = row.getJSONArray(2);
      assertEquals(1, samples.length());
      String sample = samples.getString(0);
      assertTrue(
          "sample not a member of pattern " + pattern + ": " + sample,
          universe.get(pattern).contains(sample));
    }
  }
}
