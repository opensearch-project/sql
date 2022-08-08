/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.google.errorprone.annotations.DoNotCall;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class HighlightFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() {
    String query = "SELECT Tags, highlight('Tags') FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"),
        schema("highlight('Tags')", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void accepts_unquoted_test() {
    String query = "SELECT Tags, highlight(Tags) FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("Tags", null, "text"),
        schema("highlight(Tags)", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void multiple_highlight_test() {
    String query = "SELECT highlight(Title), highlight(Body) FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Title)", null, "nested"),
        schema("highlight(Body)", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  // Enable me when * is supported
  @DoNotCall
  public void wildcard_highlight_test() {
    String query = "SELECT highlight('*itle') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*itle')", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  // Enable me when * is supported
  @DoNotCall
  public void wildcard_multi_field_highlight_test() {
    String query = "SELECT highlight('T*') FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('T*')", null, "nested"));
    var resultMap = response.getJSONArray("datarows").getJSONArray(0).getJSONObject(0);
    assertEquals(1, response.getInt("total"));
    assertTrue(resultMap.has("highlight(\"T*\").Title"));
    assertTrue(resultMap.has("highlight(\"T*\").Tags"));
  }

  // Enable me when * is supported
  @DoNotCall
  public void highlight_all_test() {
    String query = "SELECT highlight('*') FROM %s WHERE MULTI_MATCH([Title, Body], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight('*')", null, "nested"));
    assertEquals(1, response.getInt("total"));
  }

  @Test
  public void highlight_no_limit_test() {
    String query = "SELECT highlight(Body) FROM %s WHERE MATCH(Body, 'hops')";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Body)", null, "nested"));
    assertEquals(2, response.getInt("total"));
  }
}
