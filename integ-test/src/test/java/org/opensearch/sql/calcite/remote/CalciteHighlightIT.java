/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration tests for PPL highlight parameter. */
public class CalciteHighlightIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testHighlightWildcardWithSearchQuery() throws IOException {
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightContainsMatchingFragments() throws IOException {
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", "[\"*\"]");
    JSONArray highlights = result.getJSONArray("highlights");
    assertTrue("highlights array should not be empty", highlights.length() > 0);
    // At least one highlight entry should have non-empty data
    boolean foundFragment = false;
    for (int i = 0; i < highlights.length(); i++) {
      JSONObject hlEntry = highlights.getJSONObject(i);
      if (hlEntry.length() > 0) {
        foundFragment = true;
        break;
      }
    }
    assertTrue("At least one highlight entry should have fragment data", foundFragment);
  }

  @Test
  public void testHighlightOsdObjectFormat() throws IOException {
    String highlightJson =
        "{\"pre_tags\": [\"<b>\"], \"post_tags\": [\"</b>\"],"
            + " \"fields\": {\"*\": {}}, \"fragment_size\": 2147483647}";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertHighlightsExist(result);
    // Verify custom tags are applied
    JSONArray highlights = result.getJSONArray("highlights");
    boolean foundCustomTag = false;
    for (int i = 0; i < highlights.length(); i++) {
      String hlStr = highlights.getJSONObject(i).toString();
      if (hlStr.contains("<b>")) {
        foundCustomTag = true;
        break;
      }
    }
    assertTrue("Highlights should use custom <b> tags", foundCustomTag);
  }

  @Test
  public void testHighlightOsdObjectFormatWithDashboardsTags() throws IOException {
    String highlightJson =
        "{\"pre_tags\": [\"@opensearch-dashboards-highlighted-field@\"],"
            + " \"post_tags\": [\"@/opensearch-dashboards-highlighted-field@\"],"
            + " \"fields\": {\"*\": {}}, \"fragment_size\": 2147483647}";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertHighlightsExist(result);
    // Verify dashboards tags are applied
    JSONArray highlights = result.getJSONArray("highlights");
    boolean foundDashboardsTag = false;
    for (int i = 0; i < highlights.length(); i++) {
      String hlStr = highlights.getJSONObject(i).toString();
      if (hlStr.contains("@opensearch-dashboards-highlighted-field@")) {
        foundDashboardsTag = true;
        break;
      }
    }
    assertTrue("Highlights should use OSD dashboards tags", foundDashboardsTag);
  }

  @Test
  public void testHighlightWithFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | where age > 30", "[\"*\"]");
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWithFields() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | fields firstname, lastname", "[\"*\"]");
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWithFetchSize() throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    JSONObject body = new JSONObject();
    body.put("query", "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"");
    body.put("fetch_size", 5);
    body.put("highlight", new JSONArray("[\"*\"]"));
    request.setJsonEntity(body.toString());
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JSONObject result = jsonify(getResponseBody(response, true));
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWithSort() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | sort age", "[\"*\"]");
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWithEval() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source="
                + TEST_INDEX_ACCOUNT
                + " \"Holmes\" | eval age_plus_10 = age + 10 | fields firstname, age_plus_10",
            "[\"*\"]");
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWildcardInSearchText() throws IOException {
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holm*\"", "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
    assertTrue("Response should contain highlights array", result.has("highlights"));
  }

  @Test
  public void testHighlightMixedFullTextAndStructuredFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | where age > 30 | fields firstname, age",
            "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
    assertTrue("Response should contain highlights array", result.has("highlights"));
  }

  @Test
  public void testHighlightPerFieldOptions() throws IOException {
    String highlightJson =
        "{\"fields\": {\"firstname\": {\"fragment_size\": 200},"
            + " \"lastname\": {\"number_of_fragments\": 3}}}";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightSpecificFields() throws IOException {
    String highlightJson = "[\"firstname\", \"lastname\"]";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightWithHead() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | head 2", "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("Should return at most 2 rows", dataRows.length() <= 2);
    assertTrue("Response should contain highlights array", result.has("highlights"));
  }

  @Test
  public void testHighlightWithStats() throws IOException {
    // Stats aggregation with highlight - verify request succeeds
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | stats count()", "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
  }

  @Test
  public void testHighlightNoSearchQuery() throws IOException {
    // Without a search query, request should still succeed (highlights may or may not be present)
    JSONObject result = executeQueryWithHighlight("source=" + TEST_INDEX_BANK, "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
  }

  @Test
  public void testWithoutHighlightNoHighlightArray() throws IOException {
    // Without highlight parameter, highlights array should NOT appear
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK);
    assertFalse("Response should NOT contain highlights array", result.has("highlights"));
  }

  /**
   * Execute a PPL query with highlight parameter.
   *
   * @param query the PPL query string
   * @param highlightJson the highlight JSON (array or object)
   * @return the JSON response
   */
  protected JSONObject executeQueryWithHighlight(String query, String highlightJson)
      throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    JSONObject body = new JSONObject();
    body.put("query", query);
    // Parse highlightJson to proper JSON type (array or object) so it serializes correctly
    Object highlightValue =
        highlightJson.trim().startsWith("[")
            ? new JSONArray(highlightJson)
            : new JSONObject(highlightJson);
    body.put("highlight", highlightValue);
    request.setJsonEntity(body.toString());
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return jsonify(getResponseBody(response, true));
  }

  /** Assert that the response contains a non-empty highlights array. */
  private void assertHighlightsExist(JSONObject result) {
    assertTrue("Response should contain highlights array", result.has("highlights"));
    JSONArray highlights = result.getJSONArray("highlights");
    assertTrue("Highlights array should not be empty", highlights.length() > 0);
  }
}
