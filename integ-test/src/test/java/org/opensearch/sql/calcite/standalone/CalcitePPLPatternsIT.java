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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  public void testSimplePatternLabelMode() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email pattern_mode=label | head 1 | fields email, patterns_field, tokens
                   """,
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("email", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "amberduke@pyrami.com",
            "<token1>@<token2>.<token3>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("amberduke"),
                "<token2>",
                ImmutableList.of("pyrami"),
                "<token3>",
                ImmutableList.of("com"))));
  }

  @Test
  public void testSimplePatternLabelModeWithCustomPattern() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email pattern_mode=label pattern='@.*' | head 1 | fields email, patterns_field, tokens
                   """,
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("email", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "amberduke@pyrami.com",
            "amberduke<token1>",
            ImmutableMap.of("<token1>", ImmutableList.of("@pyrami.com"))));
  }

  @Test
  public void testSimplePatternAggregationMode() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email pattern_mode=aggregation pattern_max_sample_count=3",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("pattern_count", "long"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            7,
            "<token1>@<token2>.<token3>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("amberduke", "hattiebond", "nanettebates"),
                "<token2>",
                ImmutableList.of("pyrami", "netagy", "quility"),
                "<token3>",
                ImmutableList.of("com", "com", "com"))));
  }

  @Test
  public void testBrainLabelMode() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content pattern_method=BRAIN pattern_mode=label"
                    + " pattern_max_sample_count=5 variable_count_threshold=5"
                    + " frequency_threshold_percentage=0.2 | head 2 | fields content,"
                    + " patterns_field, tokens",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("content", "string"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2>"
                + " size <token3>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("10.251.31.85:50010"),
                "<token2>",
                ImmutableList.of("-7017553867379051457"),
                "<token3>",
                ImmutableList.of("67108864"))),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                + " blk_-6620182933895093708",
            "<token1> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<token2>_<token3>_r_<token4>_<token5>/part<token6>"
                + " blk_<token7>",
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("BLOCK*"),
                "<token2>",
                ImmutableList.of("200811092030"),
                "<token3>",
                ImmutableList.of("0002"),
                "<token4>",
                ImmutableList.of("000296"),
                "<token5>",
                ImmutableList.of("0"),
                "<token6>",
                ImmutableList.of("-00296."),
                "<token7>",
                ImmutableList.of("-6620182933895093708"))));
  }

  @Test
  public void testBrainAggregationMode() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content pattern_method=BRAIN pattern_mode=aggregation"
                    + " variable_count_threshold=5",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "long"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "Verification succeeded <token1> blk_<token2>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("for", "for"),
                "<token2>",
                ImmutableList.of("-1547954353065580372", "6996194389878584395"))),
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2>"
                + " size <token3>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("10.251.31.85:50010", "10.251.107.19:50010"),
                "<token3>",
                ImmutableList.of("67108864", "67108864"),
                "<token2>",
                ImmutableList.of("-7017553867379051457", "-3249711809227781266"))),
        rows(
            "<token1> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<token2>_<token3>_r_<token4>_<token5>/part<token6>"
                + " blk_<token7>",
            2,
            ImmutableMap.of(
                "<token5>",
                ImmutableList.of("0", "0"),
                "<token4>",
                ImmutableList.of("000296", "000318"),
                "<token7>",
                ImmutableList.of("-6620182933895093708", "2096692261399680562"),
                "<token6>",
                ImmutableList.of("-00296.", "-00318."),
                "<token1>",
                ImmutableList.of("BLOCK*", "BLOCK*"),
                "<token3>",
                ImmutableList.of("0002", "0002"),
                "<token2>",
                ImmutableList.of("200811092030", "200811092030"))),
        rows(
            "PacketResponder failed <token1> blk_<token2>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("for", "for"),
                "<token2>",
                ImmutableList.of("6996194389878584395", "-1547954353065580372"))));
  }

  @Test
  public void testBrainAggregationModeWithGroupByClause() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content by level pattern_method=BRAIN"
                    + " pattern_mode=aggregation pattern_max_sample_count=5"
                    + " variable_count_threshold=2 frequency_threshold_percentage=0.2",
                TEST_INDEX_HDFS_LOGS));
    System.out.println(result.getJSONArray("datarows"));
    System.out.println(result.getJSONArray("schema"));
    verifySchema(
        result,
        schema("level", "string"),
        schema("patterns_field", "string"),
        schema("pattern_count", "long"),
        schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "INFO",
            "Verification succeeded for blk_<token1>",
            2,
            ImmutableMap.of(
                "<token1>", ImmutableList.of("-1547954353065580372", "6996194389878584395"))),
        rows(
            "INFO",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to blk_<token2>"
                + " size <token3>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("10.251.31.85:50010", "10.251.107.19:50010"),
                "<token3>",
                ImmutableList.of("67108864", "67108864"),
                "<token2>",
                ImmutableList.of("-7017553867379051457", "-3249711809227781266"))),
        rows(
            "INFO",
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<token1>_<token2>_r_<token3>_<token4>/part<token5>"
                + " blk_<token6>",
            2,
            ImmutableMap.of(
                "<token5>",
                ImmutableList.of("-00296.", "-00318."),
                "<token4>",
                ImmutableList.of("0", "0"),
                "<token6>",
                ImmutableList.of("-6620182933895093708", "2096692261399680562"),
                "<token1>",
                ImmutableList.of("200811092030", "200811092030"),
                "<token3>",
                ImmutableList.of("000296", "000318"),
                "<token2>",
                ImmutableList.of("0002", "0002"))),
        rows(
            "WARN",
            "PacketResponder failed for blk_<token1>",
            2,
            ImmutableMap.of(
                "<token1>", ImmutableList.of("6996194389878584395", "-1547954353065580372"))));
  }
}
