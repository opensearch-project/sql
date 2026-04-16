/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Explain-plan integration tests for vectorSearch SQL table function. These tests verify DSL
 * push-down shape via _explain. They do NOT require the k-NN plugin since _explain only parses and
 * plans the query without executing it against a knn index.
 */
public class VectorSearchExplainIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // _explain needs the index to exist for field resolution.
    loadIndex(Index.ACCOUNT);
  }

  private static final String TEST_INDEX = TestsConstants.TEST_INDEX_ACCOUNT;

  // ── Top-k / radial DSL shape ─────────────────────────────────────────

  @Test
  public void testExplainTopKProducesKnnQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=5') AS v "
                + "LIMIT 5");

    // WrapperQueryBuilder wraps the knn JSON — verify the wrapper is present
    // and track_scores is enabled for score preservation.
    assertTrue("Explain should contain wrapper query:\n" + explain, explain.contains("wrapper"));
    assertTrue(
        "Explain should contain track_scores:\n" + explain, explain.contains("track_scores"));
  }

  @Test
  public void testExplainRadialMaxDistanceProducesKnnQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='max_distance=10.5') AS v "
                + "LIMIT 100");

    assertTrue("Explain should contain wrapper query:\n" + explain, explain.contains("wrapper"));
  }

  @Test
  public void testExplainRadialMinScoreProducesKnnQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='min_score=0.8') AS v "
                + "LIMIT 100");

    assertTrue("Explain should contain wrapper query:\n" + explain, explain.contains("wrapper"));
  }

  // ── Post-filter DSL shape ────────────────────────────────────────────

  @Test
  public void testExplainPostFilterProducesBoolQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 10");

    assertTrue("Explain should contain bool query:\n" + explain, explain.contains("bool"));
    assertTrue(
        "Explain should contain must clause (knn in scoring context):\n" + explain,
        explain.contains("must"));
    assertTrue(
        "Explain should contain filter clause (WHERE in non-scoring context):\n" + explain,
        explain.contains("filter"));
  }

  @Test
  public void testExplainCompoundPredicateProducesBoolQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10') AS v "
                + "WHERE v.state = 'TX' AND v.age > 30 "
                + "LIMIT 10");

    assertTrue("Explain should contain bool query:\n" + explain, explain.contains("bool"));
    assertTrue(
        "Explain should contain must clause (knn in scoring context):\n" + explain,
        explain.contains("must"));
    assertTrue(
        "Explain should contain filter clause (compound WHERE in non-scoring context):\n" + explain,
        explain.contains("filter"));
  }

  @Test
  public void testExplainRadialWithWhereProducesBoolQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='max_distance=10.5') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 100");

    assertTrue("Explain should contain bool query:\n" + explain, explain.contains("bool"));
    assertTrue(
        "Explain should contain must clause (knn in scoring context):\n" + explain,
        explain.contains("must"));
    assertTrue(
        "Explain should contain filter clause (WHERE in non-scoring context):\n" + explain,
        explain.contains("filter"));
  }

  // ── Sort + LIMIT explain ─────────────────────────────────────────────

  @Test
  public void testOrderByScoreDescExplainSucceeds() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='k=5') AS v "
                + "ORDER BY v._score DESC "
                + "LIMIT 5");

    assertTrue(
        "Explain should succeed with ORDER BY _score DESC:\n" + explain,
        explain.contains("wrapper"));
  }

  @Test
  public void testExplainLimitWithinKSucceeds() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='k=10') AS v "
                + "LIMIT 5");

    assertTrue("Explain should succeed with LIMIT <= k:\n" + explain, explain.contains("wrapper"));
  }

  // ── filter_type explain ─────────────────────────────────────────────

  @Test
  public void testExplainFilterTypePostProducesBoolQuery() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10,filter_type=post') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 10");

    assertTrue("Explain should contain bool query:\n" + explain, explain.contains("bool"));
    assertTrue("Explain should contain must:\n" + explain, explain.contains("must"));
    assertTrue("Explain should contain filter:\n" + explain, explain.contains("filter"));
  }

  @Test
  public void testExplainFilterTypeEfficientProducesKnnWithFilter() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='k=5,filter_type=efficient') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 5");

    // Efficient mode: knn rebuilt with filter inside, wrapped in WrapperQueryBuilder.
    // The knn JSON (including the embedded filter) is base64-encoded inside the wrapper,
    // so we verify structure by: (1) wrapper present, (2) no bool/must in plaintext
    // (that would be post-filter shape), (3) decode the base64 payload to confirm
    // the filter and predicate field are embedded inside the knn query.
    assertTrue("Explain should contain wrapper query:\n" + explain, explain.contains("wrapper"));
    assertFalse(
        "Efficient mode should not produce bool query (that is post-filter shape):\n" + explain,
        explain.contains("\"bool\""));
    assertFalse(
        "Efficient mode should not contain must clause:\n" + explain, explain.contains("\"must\""));

    // Extract and decode the base64 knn payload to verify filter embedding.
    // The explain output escapes quotes as \", so match both \" and " forms.
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("\\\\?\"query\\\\?\":\\\\?\"([A-Za-z0-9+/=]+)\\\\?\"")
            .matcher(explain);
    assertTrue("Explain should contain base64-encoded knn query:\n" + explain, m.find());
    String knnJson =
        new String(
            java.util.Base64.getDecoder().decode(m.group(1)),
            java.nio.charset.StandardCharsets.UTF_8);
    assertTrue(
        "Efficient mode knn JSON should contain filter:\n" + knnJson, knnJson.contains("filter"));
    assertTrue(
        "Efficient mode knn JSON should contain the WHERE predicate field:\n" + knnJson,
        knnJson.contains("state"));
  }

  @Test
  public void testEfficientFilterWithOrderByScoreDescSucceeds() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='k=5,filter_type=efficient') AS v "
                + "WHERE v.state = 'TX' "
                + "ORDER BY v._score DESC "
                + "LIMIT 5");

    assertTrue(
        "Explain should succeed with efficient + ORDER BY _score DESC:\n" + explain,
        explain.contains("wrapper"));
  }
}
