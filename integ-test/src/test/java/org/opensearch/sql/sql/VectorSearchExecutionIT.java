/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Happy-path execution tests for the vectorSearch() SQL table function. These tests run an actual
 * k-NN query against a small in-memory knn_vector index and assert that results come back ordered
 * by score and respect any WHERE filters.
 *
 * <p>The k-NN plugin is not provisioned by the default integ-test cluster — each test calls {@link
 * Assume#assumeTrue} on {@link #isKnnPluginInstalled()} so the class is silently skipped when k-NN
 * is absent. Run locally against a cluster that has opensearch-knn installed. Provisioning k-NN in
 * CI is a separate follow-up.
 */
public class VectorSearchExecutionIT extends SQLIntegTestCase {

  private static final String TEST_INDEX = "vector_exec_test";

  // 6 docs in 2D — two clusters so filter/radial tests have distinguishable results.
  // Cluster A near [1, 1]: docs 1-3 (state=TX, ages 25/30/40).
  // Cluster B near [9, 9]: docs 4-6 (state=CA, ages 28/35/45).
  // Pin Lucene HNSW + L2 so efficient filtering is deterministic (k-NN supports efficient
  // filtering only on lucene+hnsw and faiss+hnsw/ivf) and the L2 → 1/(1+d) scoring used by the
  // radial min_score test is well-defined.
  private static final String MAPPING =
      "{"
          + "  \"settings\": {\"index\": {\"knn\": true}},"
          + "  \"mappings\": {"
          + "    \"properties\": {"
          + "      \"embedding\": {"
          + "        \"type\": \"knn_vector\","
          + "        \"dimension\": 2,"
          + "        \"method\": {"
          + "          \"name\": \"hnsw\","
          + "          \"engine\": \"lucene\","
          + "          \"space_type\": \"l2\""
          + "        }"
          + "      },"
          + "      \"state\": {\"type\": \"keyword\"},"
          + "      \"age\": {\"type\": \"integer\"}"
          + "    }"
          + "  }"
          + "}";

  private static final String BULK_BODY =
      "{\"index\":{\"_id\":\"1\"}}\n"
          + "{\"embedding\":[1.0,1.0],\"state\":\"TX\",\"age\":25}\n"
          + "{\"index\":{\"_id\":\"2\"}}\n"
          + "{\"embedding\":[1.1,0.9],\"state\":\"TX\",\"age\":30}\n"
          + "{\"index\":{\"_id\":\"3\"}}\n"
          + "{\"embedding\":[0.9,1.2],\"state\":\"TX\",\"age\":40}\n"
          + "{\"index\":{\"_id\":\"4\"}}\n"
          + "{\"embedding\":[9.0,9.0],\"state\":\"CA\",\"age\":28}\n"
          + "{\"index\":{\"_id\":\"5\"}}\n"
          + "{\"embedding\":[9.1,8.8],\"state\":\"CA\",\"age\":35}\n"
          + "{\"index\":{\"_id\":\"6\"}}\n"
          + "{\"embedding\":[8.7,9.3],\"state\":\"CA\",\"age\":45}\n";

  @Override
  protected void init() throws Exception {
    Assume.assumeTrue("k-NN plugin not installed on test cluster", isKnnPluginInstalled());
    if (!isIndexExist(client(), TEST_INDEX)) {
      createIndexByRestClient(client(), TEST_INDEX, MAPPING);
      Request bulk = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(BULK_BODY);
      performRequest(client(), bulk);
    }
  }

  private static boolean isKnnPluginInstalled() {
    try {
      Response response = client().performRequest(new Request("GET", "/_cat/plugins?h=component"));
      String body = new String(response.getEntity().getContent().readAllBytes());
      return body.contains("opensearch-knn");
    } catch (IOException e) {
      return false;
    }
  }

  // ── Top-k happy path ────────────────────────────────────────────────

  @Test
  public void testTopKReturnsNearestSortedByScore() throws IOException {
    JSONObject result =
        executeJdbcRequest(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 1.0]', option='k=3') AS v "
                + "LIMIT 3");

    // All 3 returned docs should be from cluster A (ids 1-3), ordered by score desc.
    JSONArray rows = result.getJSONArray("datarows");
    assertEquals("Expected 3 rows:\n" + result, 3, rows.length());
    for (int i = 0; i < rows.length(); i++) {
      String id = rows.getJSONArray(i).getString(0);
      assertTrue(
          "Row " + i + " id=" + id + " should be from cluster A (1,2,3):\n" + result,
          id.equals("1") || id.equals("2") || id.equals("3"));
    }
    // Scores must be non-increasing.
    double prev = Double.POSITIVE_INFINITY;
    for (int i = 0; i < rows.length(); i++) {
      double score = rows.getJSONArray(i).getDouble(1);
      assertTrue(
          "Scores must be sorted desc, got " + score + " after " + prev + ":\n" + result,
          score <= prev);
      prev = score;
    }
  }

  // ── POST filter happy path ──────────────────────────────────────────

  @Test
  public void testPostFilterReturnsOnlyMatchingDocs() throws IOException {
    // Query from cluster B with WHERE state='TX' forces POST filtering to surface TX docs
    // (cluster A) even though the vector is closer to cluster B. k=10 covers all 6 docs so
    // post-filtering to state='TX' deterministically yields exactly {1,2,3}. filter_type=post
    // is specified explicitly because the default placement is EFFICIENT — this test
    // guarantees POST continues to work when the user opts into it.
    JSONObject result =
        executeJdbcRequest(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[9.0, 9.0]', option='k=10,filter_type=post') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 10");

    assertRowIdsEqual(result, "1", "2", "3");
  }

  // ── EFFICIENT filter happy path ─────────────────────────────────────

  @Test
  public void testEfficientFilterReturnsOnlyMatchingDocs() throws IOException {
    // Query vector sits on cluster A (TX) but WHERE state='CA' forces EFFICIENT filtering to
    // navigate HNSW toward CA docs. With k=3, a POST-filter implementation would return 0 rows
    // (the 3 nearest candidates are all TX, which get filtered out); an efficient-filter
    // implementation returns exactly the 3 CA docs {4,5,6}. This asymmetry makes the test
    // discriminate between the two filter modes.
    JSONObject result =
        executeJdbcRequest(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 1.0]', option='k=3,filter_type=efficient') AS v "
                + "WHERE v.state = 'CA' "
                + "LIMIT 3");

    assertRowIdsEqual(result, "4", "5", "6");
  }

  // ── Radial happy paths ──────────────────────────────────────────────

  @Test
  public void testRadialMaxDistanceReturnsOnlyNearDocs() throws IOException {
    // max_distance=1.0 (L2) centered on [1,1] includes all 3 cluster A docs (max L2 ≈ 0.22)
    // and excludes cluster B which is ~11 units away.
    JSONObject result =
        executeJdbcRequest(
            "SELECT v._id "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 1.0]', option='max_distance=1.0') AS v "
                + "LIMIT 10");

    assertRowIdsEqual(result, "1", "2", "3");
  }

  @Test
  public void testRadialMinScoreReturnsOnlyHighScoreDocs() throws IOException {
    // For L2 space, OpenSearch score = 1/(1+distance). Centered on [1,1], cluster A docs
    // score ~0.82-1.0 and cluster B scores ~0.08. min_score=0.5 yields exactly {1,2,3}.
    JSONObject result =
        executeJdbcRequest(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 1.0]', option='min_score=0.5') AS v "
                + "LIMIT 10");

    JSONArray rows = result.getJSONArray("datarows");
    for (int i = 0; i < rows.length(); i++) {
      double score = rows.getJSONArray(i).getDouble(1);
      assertTrue("Row " + i + " score=" + score + " should be >= 0.5:\n" + result, score >= 0.5);
    }
    assertRowIdsEqual(result, "1", "2", "3");
  }

  /** Asserts the result's datarows column 0 contains exactly the given ids (as a set). */
  private static void assertRowIdsEqual(JSONObject result, String... expectedIds) {
    JSONArray rows = result.getJSONArray("datarows");
    assertEquals(
        "Expected " + expectedIds.length + " rows:\n" + result, expectedIds.length, rows.length());
    Set<String> expected = new HashSet<>(Arrays.asList(expectedIds));
    Set<String> actual = new HashSet<>();
    for (int i = 0; i < rows.length(); i++) {
      actual.add(rows.getJSONArray(i).getString(0));
    }
    assertEquals("Row id set mismatch:\n" + result, expected, actual);
  }
}
