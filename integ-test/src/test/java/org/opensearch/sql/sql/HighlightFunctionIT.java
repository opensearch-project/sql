/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

public class HighlightFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() {
    String query = "SELECT Tags, highlight('Tags') FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"), schema("highlight('Tags')", null, "keyword"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void accepts_unquoted_test() {
    String query = "SELECT Tags, highlight(Tags) FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"), schema("highlight(Tags)", null, "keyword"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void multiple_highlight_test() {
    String query = "SELECT highlight(Title), highlight(Body) FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Title)", null, "keyword"), schema("highlight(Body)", null, "keyword"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void wildcard_highlight_test() {
    String query = "SELECT highlight('*itle') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*itle')", null, "keyword"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void highlight_all_test() {
    String query = "SELECT highlight('*') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*')", null, "keyword"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void highlight_no_limit_test() {
    String query = "SELECT highlight('*') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops')";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*')", null, "keyword"));
    assertEquals(225, response.getInt("total"));
  }
}
