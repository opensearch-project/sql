/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLPatternsIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
    loadIndex(Index.HDFS_LOGS);
  }

  @Test
  public void testSimplePattern() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email | fields email, patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "@."),
        rows("hattiebond@netagy.com", "@."),
        rows("nanettebates@quility.com", "@."),
        rows("daleadams@boink.com", "@."),
        rows("elinorratliff@scentric.com", "@."),
        rows("virginiaayala@filodyne.com", "@."),
        rows("dillardmcpherson@quailcom.com", "@."));
  }

  @Test
  public void testSimplePatternGroupByPatternsField() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email | stats count() by patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("count()", "long"), schema("patterns_field", "string"));
    verifyDataRows(result, rows(7, "@."));
  }

  @Test
  public void testSimplePatternWithCustomPattern() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns pattern='@.*' email | fields email, patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "amberduke"),
        rows("hattiebond@netagy.com", "hattiebond"),
        rows("nanettebates@quility.com", "nanettebates"),
        rows("daleadams@boink.com", "daleadams"),
        rows("elinorratliff@scentric.com", "elinorratliff"),
        rows("virginiaayala@filodyne.com", "virginiaayala"),
        rows("dillardmcpherson@quailcom.com", "dillardmcpherson"));
  }

  @Test
  public void testSimplePatternWithCustomNewField() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns new_field='username' pattern='@.*' email | fields email, username
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("username", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "amberduke"),
        rows("hattiebond@netagy.com", "hattiebond"),
        rows("nanettebates@quility.com", "nanettebates"),
        rows("daleadams@boink.com", "daleadams"),
        rows("elinorratliff@scentric.com", "elinorratliff"),
        rows("virginiaayala@filodyne.com", "virginiaayala"),
        rows("dillardmcpherson@quailcom.com", "dillardmcpherson"));
  }

  @Test
  public void testBrain() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where level='INFO' | patterns content BRAIN | fields content,"
                    + " patterns_field",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(result, schema("content", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>"),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                + " blk_-6620182933895093708",
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"),
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
                + " blk_-3249711809227781266 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>"),
        rows(
            "Verification succeeded for blk_-1547954353065580372",
            "Verification succeeded for blk_<*>"),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                + " blk_2096692261399680562",
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"),
        rows(
            "Verification succeeded for blk_6996194389878584395",
            "Verification succeeded for blk_<*>"));
  }

  @Test
  public void testBrainWithAllValidCustomParameters() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns new_field='log_pattern' variable_count_threshold=2"
                    + " frequency_threshold_percentage=0.2 content BRAIN | fields content,"
                    + " log_pattern",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(result, schema("content", "string"), schema("log_pattern", "string"));
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>"),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                + " blk_-6620182933895093708",
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"),
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added to"
                + " blk_-3249711809227781266 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>"),
        rows(
            "Verification succeeded for blk_-1547954353065580372",
            "Verification succeeded <*> blk_<*>"),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                + " blk_2096692261399680562",
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"),
        rows(
            "Verification succeeded for blk_6996194389878584395",
            "Verification succeeded <*> blk_<*>"),
        rows(
            "PacketResponder failed for blk_6996194389878584395",
            "PacketResponder failed <*> blk_<*>"),
        rows(
            "PacketResponder failed for blk_-1547954353065580372",
            "PacketResponder failed <*> blk_<*>"));
  }

  @Test
  public void testBrainWithAggregatedResult() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns variable_count_threshold=2"
                    + " frequency_threshold_percentage=0.2 content BRAIN | stats count() as count"
                    + " by patterns_field",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(result, schema("count", "long"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows(2, "Verification succeeded <*> blk_<*>"),
        rows(
            2,
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*> size"
                + " <*>"),
        rows(
            2,
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"),
        rows(2, "PacketResponder failed <*> blk_<*>"));
  }
}
