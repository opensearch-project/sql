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
import java.util.Locale;
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
    // Search for "Holmes" with wildcard highlight — _highlight should appear in results
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightContainsMatchingFragments() throws IOException {
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", "[\"*\"]");
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    // Find the _highlight column index and verify it contains highlighted fragments
    int highlightIdx = getFieldIndex(result, "_highlight");
    assertTrue("_highlight column should exist", highlightIdx >= 0);
    // At least one row should have non-empty highlight data
    boolean foundHighlight = false;
    for (int i = 0; i < dataRows.length(); i++) {
      Object hlValue = dataRows.getJSONArray(i).get(highlightIdx);
      if (hlValue != null && !hlValue.equals(JSONObject.NULL)) {
        foundHighlight = true;
        break;
      }
    }
    assertTrue("At least one row should have highlight data", foundHighlight);
  }

  @Test
  public void testHighlightOsdObjectFormat() throws IOException {
    // OSD sends highlight as a rich object with custom tags
    String highlightJson =
        "{\"pre_tags\": [\"<b>\"], \"post_tags\": [\"</b>\"],"
            + " \"fields\": {\"*\": {}}, \"fragment_size\": 2147483647}";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightOsdObjectFormatWithDashboardsTags() throws IOException {
    // The exact format OSD uses with @opensearch-dashboards-highlighted-field@ tags
    String highlightJson =
        "{\"pre_tags\": [\"@opensearch-dashboards-highlighted-field@\"],"
            + " \"post_tags\": [\"@/opensearch-dashboards-highlighted-field@\"],"
            + " \"fields\": {\"*\": {}}, \"fragment_size\": 2147483647}";
    JSONObject result =
        executeQueryWithHighlight("source=" + TEST_INDEX_ACCOUNT + " \"Holmes\"", highlightJson);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue(dataRows.length() > 0);
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightWithFilter() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | where age > 30", "[\"*\"]");
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightWithFields() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | fields firstname, lastname", "[\"*\"]");
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightWithFetchSize() throws IOException {
    // Highlight combined with fetch_size
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    String jsonBody =
        String.format(
            Locale.ROOT,
            "{\n  \"query\": \"source=%s \\\"Holmes\\\"\",\n"
                + "  \"fetch_size\": 5,\n"
                + "  \"highlight\": [\"*\"]\n}",
            TEST_INDEX_ACCOUNT);
    request.setJsonEntity(jsonBody);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JSONObject result = jsonify(getResponseBody(response, true));
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightWithSort() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source=" + TEST_INDEX_ACCOUNT + " \"Holmes\" | sort age", "[\"*\"]");
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightWithEval() throws IOException {
    JSONObject result =
        executeQueryWithHighlight(
            "source="
                + TEST_INDEX_ACCOUNT
                + " \"Holmes\" | eval age_plus_10 = age + 10 | fields firstname, age_plus_10",
            "[\"*\"]");
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testHighlightNoSearchQuery() throws IOException {
    // Without a search query, _highlight column appears but fragments may be empty
    JSONObject result = executeQueryWithHighlight("source=" + TEST_INDEX_BANK, "[\"*\"]");
    assertSchemaContains(result, "_highlight");
  }

  @Test
  public void testWithoutHighlightNoHighlightColumn() throws IOException {
    // Without highlight parameter, _highlight should NOT appear in schema
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK);
    assertSchemaDoesNotContain(result, "_highlight");
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
    request.setJsonEntity(
        String.format(
            Locale.ROOT, "{\n  \"query\": \"%s\",\n  \"highlight\": %s\n}", query, highlightJson));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return jsonify(getResponseBody(response, true));
  }

  /** Assert that the response schema contains a field with the given name. */
  private void assertSchemaContains(JSONObject result, String fieldName) {
    JSONArray schema = result.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      if (schema.getJSONObject(i).getString("name").equals(fieldName)) {
        return;
      }
    }
    Assert.fail("Schema should contain field: " + fieldName);
  }

  /** Assert that the response schema does NOT contain a field with the given name. */
  private void assertSchemaDoesNotContain(JSONObject result, String fieldName) {
    JSONArray schema = result.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      if (schema.getJSONObject(i).getString("name").equals(fieldName)) {
        Assert.fail("Schema should NOT contain field: " + fieldName);
      }
    }
  }

  /** Get the column index for a field name from the schema. */
  private int getFieldIndex(JSONObject result, String fieldName) {
    JSONArray schema = result.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      if (schema.getJSONObject(i).getString("name").equals(fieldName)) {
        return i;
      }
    }
    return -1;
  }
}
