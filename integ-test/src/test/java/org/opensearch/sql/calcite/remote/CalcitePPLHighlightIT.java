/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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

  private static final String TEST_INDEX = "highlight_test";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Create index with text fields
    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{"
            + "\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + "\"mappings\": {\"properties\": {"
            + "\"message\": {\"type\": \"text\"},"
            + "\"status\": {\"type\": \"text\"},"
            + "\"code\": {\"type\": \"integer\"}"
            + "}}"
            + "}");
    client().performRequest(createIndex);

    // Index test documents
    Request bulk = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\": {}}\n"
            + "{\"message\": \"Connection error occurred\", \"status\": \"error response\","
            + " \"code\": 500}\n"
            + "{\"index\": {}}\n"
            + "{\"message\": \"Request completed successfully\", \"status\": \"ok\", \"code\":"
            + " 200}\n"
            + "{\"index\": {}}\n"
            + "{\"message\": \"Timeout error in service\", \"status\": \"error timeout\", \"code\":"
            + " 504}\n");
    client().performRequest(bulk);
  }

  @Test
  public void testHighlightWithWildcardFields() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX + " \"error\"",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");
    assertEquals(result.getInt("size"), highlights.length());

    // At least one highlight entry should contain "error" wrapped in tags
    boolean foundHighlight = false;
    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        String hlStr = highlights.get(i).toString();
        if (hlStr.contains("<em>error</em>") || hlStr.contains("<em>Error</em>")) {
          foundHighlight = true;
          break;
        }
      }
    }
    assertTrue("Expected at least one highlight with <em> tags", foundHighlight);
  }

  @Test
  public void testHighlightWithSpecificField() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX + " \"error\"",
            "{\"fields\": {\"message\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\":"
                + " [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");

    // Check that highlights only contain "message" field, not "status"
    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        JSONObject hl = highlights.getJSONObject(i);
        assertFalse("Should not highlight status field", hl.has("status"));
      }
    }
  }

  @Test
  public void testHighlightWithCustomTags() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX + " \"error\"",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<mark>\"], \"post_tags\": [\"</mark>\"]}");

    assertTrue(result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");

    boolean foundCustomTag = false;
    for (int i = 0; i < highlights.length(); i++) {
      if (!highlights.isNull(i)) {
        String hlStr = highlights.get(i).toString();
        if (hlStr.contains("<mark>")) {
          foundCustomTag = true;
          break;
        }
      }
    }
    assertTrue("Expected custom <mark> tags in highlights", foundCustomTag);
  }

  @Test
  public void testNoHighlightWhenNotRequested() throws IOException {
    JSONObject result = executeQuery("search source=" + TEST_INDEX + " \"error\"");

    assertFalse("Should not have highlights when not requested", result.has("highlights"));
  }

  @Test
  public void testHighlightWithPipedFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "search source=" + TEST_INDEX + " \"error\" | where code > 500",
            "{\"fields\": {\"*\": {}}, \"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}");

    assertTrue(result.has("highlights"));
    // Only the doc with code=504 should match
    assertEquals(1, result.getInt("size"));
    JSONArray highlights = result.getJSONArray("highlights");
    assertEquals(1, highlights.length());
    assertNotNull(highlights.get(0));
  }

  @Test
  public void testExplainWithHighlight() throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl/_explain");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"query\": \"search source=%s \\\"error\\\"\","
                + "\"highlight\": {\"fields\": {\"*\": {}},"
                + "\"pre_tags\": [\"<em>\"], \"post_tags\": [\"</em>\"]}}",
            TEST_INDEX));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());

    String body = org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true);
    // The explain output should contain the highlight clause
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
}
