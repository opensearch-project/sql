/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.LikeQueryIT;

public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    super.test_convert_field_text_to_keyword();
  }

  @Test
  public void test_like_should_be_case_sensitive() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyNumOfRows(result, 0);
  }

  @Test
  public void test_ilike_is_case_insensitive() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE ILike(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(
        result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
  }

  @Test
  public void test_like_with_case_sensitive() throws IOException {
    // only work in v3
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%', false) | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(
        result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
    query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%', true) | fields KeywordBody";
    result = executeQuery(query);
    verifyNumOfRows(result, 0);
  }

  @Test
  public void test_the_default_3rd_option() throws IOException {
    // only work in v3
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "true",
        () -> {
          try {
            JSONObject result = executeQuery(query);
            verifyDataRows(
                result,
                rows("test wildcard"),
                rows("test wildcard in the end of the text%"),
                rows("test wildcard in % the middle of the text"),
                rows("test wildcard %% beside each other"),
                rows("test wildcard in the end of the text_"),
                rows("test wildcard in _ the middle of the text"),
                rows("test wildcard __ beside each other"));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          try {
            JSONObject result = executeQuery(query);
            verifyNumOfRows(result, 0);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
