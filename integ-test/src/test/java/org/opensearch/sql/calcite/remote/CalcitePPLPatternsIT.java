/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HDFS_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLPatternsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.WEBLOG);
    loadIndex(Index.HDFS_LOGS);
  }

  @Test
  public void testSimplePatternLabelMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label | head 1 | fields email, patterns_field",
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("amberduke@pyrami.com", "<*>@<*>.<*>"));
  }

  @Test
  public void testSimplePatternLabelMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label show_numbered_token=true | head 1 | fields"
                    + " email, patterns_field, tokens",
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
  public void testSimplePatternLabelMode_NullFieldReturnEmpty() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns message | where isnull(message) | fields message,"
                    + " patterns_field",
                TEST_INDEX_WEBLOGS));
    verifySchema(result, schema("message", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows(null, ""), rows(null, ""));
  }

  @Test
  public void testSimplePatternLabelMode_EmptyStringReturnEmpty() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | eval message2 = '' | head 1 | patterns message2 | fields message2,"
                    + " patterns_field",
                TEST_INDEX_WEBLOGS));
    verifySchema(result, schema("message2", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("", ""));
  }

  @Test
  public void testSimplePatternLabelModeWithCustomPattern_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | patterns email mode=label show_numbered_token=true pattern='@.*' |"
                    + " head 1 | fields email, patterns_field, tokens",
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
  public void testSimplePatternAggregationMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email mode=aggregation max_sample_count=3", TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("pattern_count", "bigint"),
        schema("patterns_field", "string"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            "<*>@<*>.<*>",
            7,
            ImmutableList.of(
                "amberduke@pyrami.com", "hattiebond@netagy.com", "nanettebates@quility.com")));
  }

  @Test
  public void testSimplePatternAggregationMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email mode=aggregation max_sample_count=3"
                    + " show_numbered_token=true",
                TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("pattern_count", "bigint"),
        schema("patterns_field", "string"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            "<token1>@<token2>.<token3>",
            7,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("amberduke", "hattiebond", "nanettebates"),
                "<token2>",
                ImmutableList.of("pyrami", "netagy", "quility"),
                "<token3>",
                ImmutableList.of("com", "com", "com")),
            ImmutableList.of(
                "amberduke@pyrami.com", "hattiebond@netagy.com", "nanettebates@quility.com")));
  }

  @Test
  public void testSimplePatternAggregationMode_WithGroupBy_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns email by male mode=aggregation max_sample_count=1"
                    + " show_numbered_token=true",
                TEST_INDEX_BANK));
    verifySchemaInOrder(
        result,
        schema("male", "boolean"),
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            false,
            "<token1>@<token2>.<token3>",
            3,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("nanettebates"),
                "<token2>",
                ImmutableList.of("quility"),
                "<token3>",
                ImmutableList.of("com")),
            ImmutableList.of("nanettebates@quility.com")),
        rows(
            true,
            "<token1>@<token2>.<token3>",
            4,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("amberduke"),
                "<token2>",
                ImmutableList.of("pyrami"),
                "<token3>",
                ImmutableList.of("com")),
            ImmutableList.of("amberduke@pyrami.com")));
  }

  @Test
  public void testBrainLabelMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=BRAIN mode=label"
                    + " max_sample_count=5 variable_count_threshold=5"
                    + " frequency_threshold_percentage=0.2 | head 2 | fields content,"
                    + " patterns_field",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(result, schema("content", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                + " blk_-7017553867379051457 size 67108864",
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*>"
                + " size <*>"),
        rows(
            "BLOCK* NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                + " blk_-6620182933895093708",
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*> blk_<*>"));
  }

  @Test
  public void testBrainLabelMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=BRAIN mode=label"
                    + " max_sample_count=5 show_numbered_token=true variable_count_threshold=5"
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
  public void testBrainAggregationMode_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=brain mode=aggregation"
                    + " variable_count_threshold=5",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            "Verification succeeded <*> blk_<*>",
            2,
            ImmutableList.of(
                "Verification succeeded for blk_-1547954353065580372",
                "Verification succeeded for blk_6996194389878584395")),
        rows(
            "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*>"
                + " size <*>",
            2,
            ImmutableList.of(
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                    + " blk_-7017553867379051457 size 67108864",
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added"
                    + " to blk_-3249711809227781266 size 67108864")),
        rows(
            "<*> NameSystem.allocateBlock:"
                + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*>"
                + " blk_<*>",
            2,
            ImmutableList.of(
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                    + " blk_-6620182933895093708",
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                    + " blk_2096692261399680562")),
        rows(
            "PacketResponder failed <*> blk_<*>",
            2,
            ImmutableList.of(
                "PacketResponder failed for blk_6996194389878584395",
                "PacketResponder failed for blk_-1547954353065580372")));
  }

  @Test
  public void testBrainAggregationMode_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content method=brain mode=aggregation"
                    + " show_numbered_token=true variable_count_threshold=5",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            "Verification succeeded <token1> blk_<token2>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("for", "for"),
                "<token2>",
                ImmutableList.of("-1547954353065580372", "6996194389878584395")),
            ImmutableList.of(
                "Verification succeeded for blk_-1547954353065580372",
                "Verification succeeded for blk_6996194389878584395")),
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
                ImmutableList.of("-7017553867379051457", "-3249711809227781266")),
            ImmutableList.of(
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                    + " blk_-7017553867379051457 size 67108864",
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added"
                    + " to blk_-3249711809227781266 size 67108864")),
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
                ImmutableList.of("200811092030", "200811092030")),
            ImmutableList.of(
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                    + " blk_-6620182933895093708",
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                    + " blk_2096692261399680562")),
        rows(
            "PacketResponder failed <token1> blk_<token2>",
            2,
            ImmutableMap.of(
                "<token1>",
                ImmutableList.of("for", "for"),
                "<token2>",
                ImmutableList.of("6996194389878584395", "-1547954353065580372")),
            ImmutableList.of(
                "PacketResponder failed for blk_6996194389878584395",
                "PacketResponder failed for blk_-1547954353065580372")));
  }

  @Test
  public void testBrainAggregationModeWithGroupByClause_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | patterns content by level method=BRAIN"
                    + " mode=aggregation show_numbered_token=true max_sample_count=5"
                    + " variable_count_threshold=2 frequency_threshold_percentage=0.2",
                TEST_INDEX_HDFS_LOGS));
    verifySchema(
        result,
        schema("level", "string"),
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("tokens", "struct"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result,
        rows(
            "INFO",
            "Verification succeeded for blk_<token1>",
            2,
            ImmutableMap.of(
                "<token1>", ImmutableList.of("-1547954353065580372", "6996194389878584395")),
            ImmutableList.of(
                "Verification succeeded for blk_-1547954353065580372",
                "Verification succeeded for blk_6996194389878584395")),
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
                ImmutableList.of("-7017553867379051457", "-3249711809227781266")),
            ImmutableList.of(
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added to"
                    + " blk_-7017553867379051457 size 67108864",
                "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added"
                    + " to blk_-3249711809227781266 size 67108864")),
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
                ImmutableList.of("0002", "0002")),
            ImmutableList.of(
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                    + " blk_-6620182933895093708",
                "BLOCK* NameSystem.allocateBlock:"
                    + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                    + " blk_2096692261399680562")),
        rows(
            "WARN",
            "PacketResponder failed for blk_<token1>",
            2,
            ImmutableMap.of(
                "<token1>", ImmutableList.of("6996194389878584395", "-1547954353065580372")),
            ImmutableList.of(
                "PacketResponder failed for blk_6996194389878584395",
                "PacketResponder failed for blk_-1547954353065580372")));
  }

  @Test
  public void testBrainParseWithUUID_NotShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d"
                    + " user_currency=USD' | head 1 | patterns body method=BRAIN mode=label |"
                    + " fields patterns_field",
                Index.WEBLOG.getName()));
    verifySchema(result, schema("patterns_field", "string"));
    verifyDataRows(result, rows("[PlaceOrder] user_id=<*UUID*> user_currency=USD"));
  }

  @Test
  public void testBrainParseWithUUID_ShowNumberedToken() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval body = '[PlaceOrder] user_id=d664d7be-77d8-11f0-8880-0242f00b101d"
                    + " user_currency=USD' | head 1 | patterns body method=BRAIN mode=label"
                    + " show_numbered_token=true | fields patterns_field, tokens",
                Index.WEBLOG.getName()));
    verifySchema(result, schema("patterns_field", "string"), schema("tokens", "struct"));
    verifyDataRows(
        result,
        rows(
            "[PlaceOrder] user_id=<token1> user_currency=USD",
            ImmutableMap.of("<token1>", ImmutableList.of("d664d7be-77d8-11f0-8880-0242f00b101d"))));
  }

  @Test
  public void testBrainAggregationMode_UDAFPushdown_NotShowNumberedToken() throws IOException {
    // Test UDAF pushdown for patterns BRAIN aggregation mode
    // This verifies that the query is pushed down to OpenSearch as a scripted metric aggregation
    // UDAF pushdown is disabled by default, enable it for this test
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_UDAF_PUSHDOWN_ENABLED.getKeyValue(), "true"));
    try {
      JSONObject result =
          executeQuery(
              String.format(
                  "source=%s | patterns content method=brain mode=aggregation"
                      + " variable_count_threshold=5",
                  TEST_INDEX_HDFS_LOGS));

      // Verify schema matches expected output
      verifySchema(
          result,
          schema("patterns_field", "string"),
          schema("pattern_count", "bigint"),
          schema("sample_logs", "array"));

      // Verify data rows - should match the non-pushdown results
      verifyDataRows(
          result,
          rows(
              "Verification succeeded <*> blk_<*>",
              2,
              ImmutableList.of(
                  "Verification succeeded for blk_-1547954353065580372",
                  "Verification succeeded for blk_6996194389878584395")),
          rows(
              "BLOCK* NameSystem.addStoredBlock: blockMap updated: <*IP*> is added to blk_<*>"
                  + " size <*>",
              2,
              ImmutableList.of(
                  "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added"
                      + " to blk_-7017553867379051457 size 67108864",
                  "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added"
                      + " to blk_-3249711809227781266 size 67108864")),
          rows(
              "<*> NameSystem.allocateBlock:"
                  + " /user/root/sortrand/_temporary/_task_<*>_<*>_r_<*>_<*>/part<*>"
                  + " blk_<*>",
              2,
              ImmutableList.of(
                  "BLOCK* NameSystem.allocateBlock:"
                      + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                      + " blk_-6620182933895093708",
                  "BLOCK* NameSystem.allocateBlock:"
                      + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                      + " blk_2096692261399680562")),
          rows(
              "PacketResponder failed <*> blk_<*>",
              2,
              ImmutableList.of(
                  "PacketResponder failed for blk_6996194389878584395",
                  "PacketResponder failed for blk_-1547954353065580372")));
    } finally {
      updateClusterSettings(
          new ClusterSetting(
              "persistent", Settings.Key.CALCITE_UDAF_PUSHDOWN_ENABLED.getKeyValue(), "false"));
    }
  }

  @Test
  public void testBrainAggregationMode_UDAFPushdown_ShowNumberedToken() throws IOException {
    // Test UDAF pushdown for patterns BRAIN aggregation mode with numbered tokens
    // UDAF pushdown is disabled by default, enable it for this test
    updateClusterSettings(
        new ClusterSetting(
            "persistent", Settings.Key.CALCITE_UDAF_PUSHDOWN_ENABLED.getKeyValue(), "true"));
    try {
      JSONObject result =
          executeQuery(
              String.format(
                  "source=%s | patterns content method=brain mode=aggregation"
                      + " show_numbered_token=true variable_count_threshold=5",
                  TEST_INDEX_HDFS_LOGS));

      // Verify schema includes tokens field
      verifySchema(
          result,
          schema("patterns_field", "string"),
          schema("pattern_count", "bigint"),
          schema("tokens", "struct"),
          schema("sample_logs", "array"));

      // Verify data rows with tokens
      verifyDataRows(
          result,
          rows(
              "Verification succeeded <token1> blk_<token2>",
              2,
              ImmutableMap.of(
                  "<token1>",
                  ImmutableList.of("for", "for"),
                  "<token2>",
                  ImmutableList.of("-1547954353065580372", "6996194389878584395")),
              ImmutableList.of(
                  "Verification succeeded for blk_-1547954353065580372",
                  "Verification succeeded for blk_6996194389878584395")),
          rows(
              "BLOCK* NameSystem.addStoredBlock: blockMap updated: <token1> is added to"
                  + " blk_<token2> size <token3>",
              2,
              ImmutableMap.of(
                  "<token1>",
                  ImmutableList.of("10.251.31.85:50010", "10.251.107.19:50010"),
                  "<token3>",
                  ImmutableList.of("67108864", "67108864"),
                  "<token2>",
                  ImmutableList.of("-7017553867379051457", "-3249711809227781266")),
              ImmutableList.of(
                  "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.31.85:50010 is added"
                      + " to blk_-7017553867379051457 size 67108864",
                  "BLOCK* NameSystem.addStoredBlock: blockMap updated: 10.251.107.19:50010 is added"
                      + " to blk_-3249711809227781266 size 67108864")),
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
                  ImmutableList.of("200811092030", "200811092030")),
              ImmutableList.of(
                  "BLOCK* NameSystem.allocateBlock:"
                      + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000296_0/part-00296."
                      + " blk_-6620182933895093708",
                  "BLOCK* NameSystem.allocateBlock:"
                      + " /user/root/sortrand/_temporary/_task_200811092030_0002_r_000318_0/part-00318."
                      + " blk_2096692261399680562")),
          rows(
              "PacketResponder failed <token1> blk_<token2>",
              2,
              ImmutableMap.of(
                  "<token1>",
                  ImmutableList.of("for", "for"),
                  "<token2>",
                  ImmutableList.of("6996194389878584395", "-1547954353065580372")),
              ImmutableList.of(
                  "PacketResponder failed for blk_6996194389878584395",
                  "PacketResponder failed for blk_-1547954353065580372")));
    } finally {
      updateClusterSettings(
          new ClusterSetting(
              "persistent", Settings.Key.CALCITE_UDAF_PUSHDOWN_ENABLED.getKeyValue(), "false"));
    }
  }
}
