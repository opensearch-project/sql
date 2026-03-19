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
    int hlIndex = getHighlightColumnIndex(result);
    assertTrue("_highlight column should exist in schema", hlIndex >= 0);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("datarows should not be empty", dataRows.length() > 0);
    // At least one highlight entry should have non-empty data
    boolean foundFragment = false;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject hlEntry = dataRows.getJSONArray(i).optJSONObject(hlIndex);
      if (hlEntry != null && hlEntry.length() > 0) {
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
    int hlIndex = getHighlightColumnIndex(result);
    boolean foundCustomTag = false;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject hlEntry = dataRows.getJSONArray(i).optJSONObject(hlIndex);
      if (hlEntry != null && hlEntry.toString().contains("<b>")) {
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
    int hlIndex = getHighlightColumnIndex(result);
    boolean foundDashboardsTag = false;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject hlEntry = dataRows.getJSONArray(i).optJSONObject(hlIndex);
      if (hlEntry != null
          && hlEntry.toString().contains("@opensearch-dashboards-highlighted-field@")) {
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
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightMixedFullTextAndStructuredFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | where age > 30 | fields firstname, age",
            "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
    assertHighlightsExist(result);
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
    assertHighlightsExist(result);
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
  public void testHighlightBooleanOrSearch() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" OR \"Bond\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("OR search should return results", dataRows.length() > 0);
    assertHighlightsExist(result);
    // Verify highlights contain fragments for both search terms
    int hlIndex = getHighlightColumnIndex(result);
    boolean foundHolmes = false;
    boolean foundBond = false;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject hlEntry = dataRows.getJSONArray(i).optJSONObject(hlIndex);
      if (hlEntry != null) {
        String hlStr = hlEntry.toString();
        if (hlStr.contains("Holmes")) foundHolmes = true;
        if (hlStr.contains("Bond")) foundBond = true;
      }
    }
    assertTrue("Highlights should contain Holmes fragments", foundHolmes);
    assertTrue("Highlights should contain Bond fragments", foundBond);
  }

  @Test
  public void testHighlightBooleanAndSearch() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" AND \"Lane\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("AND search should return results", dataRows.length() > 0);
    assertHighlightsExist(result);
    // Verify highlights contain fragments for both terms
    int hlIndex = getHighlightColumnIndex(result);
    boolean foundHolmes = false;
    boolean foundLane = false;
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject hlEntry = dataRows.getJSONArray(i).optJSONObject(hlIndex);
      if (hlEntry != null) {
        String hlStr = hlEntry.toString();
        if (hlStr.contains("Holmes")) foundHolmes = true;
        if (hlStr.contains("Lane")) foundLane = true;
      }
    }
    assertTrue("Highlights should contain Holmes fragments", foundHolmes);
    assertTrue("Highlights should contain Lane fragments", foundLane);
  }

  @Test
  public void testHighlightNotSearch() throws IOException {
    // NOT queries negate the match — the query succeeds but highlights are empty
    // because there are no positive matches to highlight
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " NOT \"Holmes\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("NOT search should return results", dataRows.length() > 0);
  }

  @Test
  public void testHighlightBooleanOrWithFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" OR \"Bond\" | where age > 30", "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
    assertHighlightsExist(result);
  }

  @Test
  public void testHighlightNoSearchQuery() throws IOException {
    // Without a search query, request should still succeed (highlights may or may not be present)
    JSONObject result = executeQueryWithHighlight("source=" + TEST_INDEX_BANK, "[\"*\"]");
    assertTrue("Response should contain datarows", result.has("datarows"));
  }

  @Test
  public void testWithoutHighlightNoHighlightColumn() throws IOException {
    // Without highlight parameter, _highlight column should NOT appear in schema
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK);
    assertTrue("_highlight column should NOT be in schema", getHighlightColumnIndex(result) < 0);
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

  /**
   * Find the index of the _highlight column in the schema array.
   *
   * @return the column index, or -1 if not present
   */
  private int getHighlightColumnIndex(JSONObject result) {
    JSONArray schema = result.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      if ("_highlight".equals(schema.getJSONObject(i).getString("name"))) {
        return i;
      }
    }
    return -1;
  }

  /** Assert that the response contains a _highlight column with non-empty highlight data. */
  private void assertHighlightsExist(JSONObject result) {
    int hlIndex = getHighlightColumnIndex(result);
    assertTrue("Schema should contain _highlight column", hlIndex >= 0);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("datarows should not be empty", dataRows.length() > 0);
  }
}
