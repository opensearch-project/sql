/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class LikeQueryIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_like_with_percent() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test wildcard%') | fields KeywordBody";
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
  public void test_like_with_escaped_percent() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, '\\\\%test wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_like_in_where_with_escaped_underscore() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, '\\\\_test wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("_test wildcard in the beginning of the text"));
  }

  @Test
  public void test_like_on_text_field_with_one_word() throws IOException {
    String query =
        "source=" + TEST_INDEX_WILDCARD + " | WHERE Like(TextBody, 'test*') | fields TextBody";
    JSONObject result = executeQuery(query);
    assertEquals(9, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_keyword_field_with_one_word() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(TextKeywordBody, 'test*') | fields TextKeywordBody";
    JSONObject result = executeQuery(query);
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_keyword_field_with_greater_than_one_word() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(TextKeywordBody, 'test wild*') | fields TextKeywordBody";
    JSONObject result = executeQuery(query);
    assertEquals(7, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_field_with_greater_than_one_word() throws IOException {
    String query =
        "source=" + TEST_INDEX_WILDCARD + " | WHERE Like(TextBody, 'test wild*') | fields TextBody";
    JSONObject result = executeQuery(query);
    assertEquals(0, result.getInt("total"));
  }

  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(TextKeywordBody, '*') | fields TextKeywordBody";
    String result = explainQueryToString(query);
    assertTrue(result.contains("TextKeywordBody.keyword"));
  }
}
