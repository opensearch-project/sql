/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLHighlightIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testHighlightWithWildcardFields() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX_ACCOUNT + " \\\"Street\\\"",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");
    assertEquals(result.getInt("size"), highlights.length());

    boolean foundHighlight = false;
    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        JSONObject hl = highlights.getJSONObject(i);
        for (String key : hl.keySet()) {
          String fragment = hl.getJSONArray(key).getString(0);
          if (fragment.contains("<em>") && fragment.contains("Street")) {
            foundHighlight = true;
            break;
          }
        }
        if (foundHighlight) break;
      }
    }
    assertTrue("Expected at least one highlight with <em> tags", foundHighlight);
  }

  @Test
  public void testHighlightWithSpecificField() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX_ACCOUNT + " \\\"Street\\\"",
            "{\"fields\": {\"address\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\":"
                + " [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");

    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        JSONObject hl = highlights.getJSONObject(i);
        // Only address field should be highlighted, not other text fields
        assertFalse("Should not highlight firstname field", hl.has("firstname"));
      }
    }
  }

  @Test
  public void testHighlightWithCustomTags() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX_ACCOUNT + " \\\"Street\\\"",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<mark>\"], \"post_tags\":"
                + " [\"</mark>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");

    boolean foundCustomTag = false;
    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        JSONObject hl = highlights.getJSONObject(i);
        for (String key : hl.keySet()) {
          String fragment = hl.getJSONArray(key).getString(0);
          if (fragment.contains("<mark>")) {
            foundCustomTag = true;
            break;
          }
        }
        if (foundCustomTag) break;
      }
    }
    assertTrue("Expected custom <mark> tags in highlights", foundCustomTag);
  }

  @Test
  public void testNoHighlightWhenNotRequested() throws IOException {
    JSONObject result =
        executeQueryNoHighlight("search source=" + TEST_INDEX_ACCOUNT + " \\\"Street\\\"");

    assertFalse("Should not have highlights when not requested", result.has("highlights"));
  }

  @Test
  public void testHighlightWithPipedFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX_ACCOUNT + " \\\"Street\\\" | where age > 30",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    assertTrue(result.getInt("size") > 0);
    JSONArray highlights = result.getJSONArray("highlights");
    assertEquals(result.getInt("size"), highlights.length());
  }

  @Test
  public void testExplainWithHighlight() throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl/_explain");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"query\": \"search source=%s \\\"Street\\\"\","
                + "\"highlight\": {\"fields\": {\"*\": {}},"
                + "\"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}}",
            TEST_INDEX_ACCOUNT));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    assertTrue("Explain should contain highlight", body.contains("highlight"));
  }

  private JSONObject executeQueryWithHighlight(String query, String highlightJson)
      throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"highlight\": %s}", query, highlightJson));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }

  private JSONObject executeQueryNoHighlight(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    return new JSONObject(body);
  }
}
