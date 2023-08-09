/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class LikeQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  @Test
  public void test_like_in_select() throws IOException {
    String query = "SELECT KeywordBody, KeywordBody LIKE 'test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", true),
        rows("test wildcard in the end of the text%", true),
        rows("%test wildcard in the beginning of the text", false),
        rows("test wildcard in % the middle of the text", true),
        rows("test wildcard %% beside each other", true),
        rows("test wildcard in the end of the text_", true),
        rows("_test wildcard in the beginning of the text", false),
        rows("test wildcard in _ the middle of the text", true),
        rows("test wildcard __ beside each other", true),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_select_with_escaped_percent() throws IOException {
    String query = "SELECT KeywordBody, KeywordBody LIKE '\\\\%test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", false),
        rows("test wildcard in the end of the text%", false),
        rows("%test wildcard in the beginning of the text", true),
        rows("test wildcard in % the middle of the text", false),
        rows("test wildcard %% beside each other", false),
        rows("test wildcard in the end of the text_", false),
        rows("_test wildcard in the beginning of the text", false),
        rows("test wildcard in _ the middle of the text", false),
        rows("test wildcard __ beside each other", false),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_select_with_escaped_underscore() throws IOException {
    String query = "SELECT KeywordBody, KeywordBody LIKE '\\\\_test wildcard%' FROM " + TEST_INDEX_WILDCARD;
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard", false),
        rows("test wildcard in the end of the text%", false),
        rows("%test wildcard in the beginning of the text", false),
        rows("test wildcard in % the middle of the text", false),
        rows("test wildcard %% beside each other", false),
        rows("test wildcard in the end of the text_", false),
        rows("_test wildcard in the beginning of the text", true),
        rows("test wildcard in _ the middle of the text", false),
        rows("test wildcard __ beside each other", false),
        rows("test backslash wildcard \\_", false));
  }

  @Test
  public void test_like_in_where() throws IOException {
    String query = "SELECT KeywordBody FROM " + TEST_INDEX_WILDCARD + " WHERE KeywordBody LIKE 'test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
  }

  @Test
  public void test_like_in_where_with_escaped_percent() throws IOException {
    String query = "SELECT KeywordBody FROM " + TEST_INDEX_WILDCARD + " WHERE KeywordBody LIKE '\\\\%test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("%test wildcard in the beginning of the text"));
  }

  @Test
  public void test_like_in_where_with_escaped_underscore() throws IOException {
    String query = "SELECT KeywordBody FROM " + TEST_INDEX_WILDCARD + " WHERE KeywordBody LIKE '\\\\_test wildcard%'";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(result,
        rows("_test wildcard in the beginning of the text"));
  }

  @Test
  public void test_like_on_text_field_with_one_word() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextBody LIKE 'test*'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(9, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_keyword_field_with_one_word() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextKeywordBody LIKE 'test*'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_keyword_field_with_greater_than_one_word() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextKeywordBody LIKE 'test wild*'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(7, result.getInt("total"));
  }

  @Test
  public void test_like_on_text_field_with_greater_than_one_word() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextBody LIKE 'test wild*'";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(0, result.getInt("total"));
  }

  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_WILDCARD + " WHERE TextKeywordBody LIKE '*'";
    String result = explainQuery(query);
    assertTrue(result.contains("TextKeywordBody.keyword"));
  }
}
