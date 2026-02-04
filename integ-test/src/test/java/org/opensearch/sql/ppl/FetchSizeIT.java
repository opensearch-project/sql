/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

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

/** Integration tests for PPL fetch_size parameter. */
public class FetchSizeIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testFetchSizeLimitsResults() throws IOException {
    // accounts index has 1000 documents, request only 5
    JSONObject result = executeQueryWithFetchSize("source=" + TEST_INDEX_ACCOUNT, 5);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(5, dataRows.length());
  }

  @Test
  public void testFetchSizeWithFields() throws IOException {
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | fields firstname, age", TEST_INDEX_ACCOUNT), 3);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(3, dataRows.length());
  }

  @Test
  public void testFetchSizeWithFilter() throws IOException {
    // Filter + fetch_size
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | where age > 30", TEST_INDEX_ACCOUNT), 10);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(10, dataRows.length());
  }

  @Test
  public void testFetchSizeWithSort() throws IOException {
    // Sort + fetch_size - should get the first N results after sorting
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | sort age | fields firstname, age", TEST_INDEX_ACCOUNT), 5);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(5, dataRows.length());
  }

  @Test
  public void testFetchSizeWithEval() throws IOException {
    // Eval command + fetch_size - ensures fetch_size works with post-processing commands
    JSONObject result =
        executeQueryWithFetchSize(
            String.format(
                "source=%s | eval age_plus_10 = age + 10 | fields firstname, age, age_plus_10",
                TEST_INDEX_ACCOUNT),
            7);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(7, dataRows.length());
  }

  @Test
  public void testFetchSizeWithDedup() throws IOException {
    // Dedup command + fetch_size - dedup may return fewer than fetch_size if not enough unique
    // values
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | dedup gender | fields gender", TEST_INDEX_ACCOUNT), 100);
    JSONArray dataRows = result.getJSONArray("datarows");
    // There are only 2 genders (M, F) in the dataset, so we should get at most 2
    assertTrue(dataRows.length() <= 2);
  }

  @Test
  public void testFetchSizeWithRename() throws IOException {
    JSONObject result =
        executeQueryWithFetchSize(
            String.format(
                "source=%s | rename firstname as first_name | fields first_name, age",
                TEST_INDEX_ACCOUNT),
            4);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(4, dataRows.length());
  }

  @Test
  public void testFetchSizeZeroReturnsAllResults() throws IOException {
    // fetch_size=0 should be treated as "no limit" (use system default)
    JSONObject result = executeQueryWithFetchSize(String.format("source=%s", TEST_INDEX_BANK), 0);
    JSONArray dataRows = result.getJSONArray("datarows");
    // Bank index has 7 documents
    assertEquals(7, dataRows.length());
  }

  @Test
  public void testFetchSizeLargerThanDataset() throws IOException {
    // When fetch_size is larger than the dataset, return all available results
    JSONObject result =
        executeQueryWithFetchSize(String.format("source=%s", TEST_INDEX_BANK), 1000);
    JSONArray dataRows = result.getJSONArray("datarows");
    // Bank index has 7 documents, so we should get 7, not 1000
    assertEquals(7, dataRows.length());
  }

  @Test
  public void testFetchSizeOne() throws IOException {
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | fields firstname", TEST_INDEX_ACCOUNT), 1);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(1, dataRows.length());
  }

  @Test
  public void testFetchSizeWithStats() throws IOException {
    // Stats aggregation - fetch_size should still apply to aggregation results
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | stats count() by gender", TEST_INDEX_ACCOUNT), 100);
    JSONArray dataRows = result.getJSONArray("datarows");
    // Stats by gender should return 2 rows (M and F)
    assertEquals(2, dataRows.length());
  }

  @Test
  public void testFetchSizeWithHead() throws IOException {
    // Both head command and fetch_size - the smaller limit should win
    // head 3 limits to 3, fetch_size 10 would allow 10, so we get 3
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | head 3 | fields firstname", TEST_INDEX_ACCOUNT), 10);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(3, dataRows.length());
  }

  @Test
  public void testFetchSizeSmallerThanHead() throws IOException {
    // fetch_size smaller than head - fetch_size should further limit
    // head 100 would return 100, but fetch_size 5 limits to 5
    JSONObject result =
        executeQueryWithFetchSize(
            String.format("source=%s | head 100 | fields firstname", TEST_INDEX_ACCOUNT), 5);
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(5, dataRows.length());
  }

  @Test
  public void testWithoutFetchSizeReturnsDefaultBehavior() throws IOException {
    // Without fetch_size, should return results up to system default
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_BANK));
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(7, dataRows.length());
  }

  /**
   * Execute a PPL query with fetch_size parameter.
   *
   * @param query the PPL query string
   * @param fetchSize the maximum number of results to return
   * @return the JSON response
   */
  protected JSONObject executeQueryWithFetchSize(String query, int fetchSize) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    String jsonBody =
        String.format(
            Locale.ROOT, "{\n  \"query\": \"%s\",\n  \"fetch_size\": %d\n}", query, fetchSize);
    request.setJsonEntity(jsonBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return jsonify(getResponseBody(response, true));
  }
}
