/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Integration tests for vectorSearch SQL table function. These tests verify DSL push-down shape via
 * _explain and validation error paths. They do NOT require the k-NN plugin since _explain only
 * parses and plans the query without executing it against a knn index.
 */
public class VectorSearchIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // _explain needs the index to exist for field resolution.
    loadIndex(Index.ACCOUNT);
  }

  private static final String TEST_INDEX = TestsConstants.TEST_INDEX_ACCOUNT;

  // ── DSL shape verification via _explain ───────────────────────────────

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

  // ── Validation error paths ────────────────────────────────────────────

  @Test
  public void testMutualExclusivityRejectsKAndMaxDistance() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=5,max_distance=10') AS v"));

    assertThat(ex.getMessage(), containsString("Only one of"));
  }

  @Test
  public void testMutualExclusivityRejectsKAndMinScore() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=5,min_score=0.5') AS v"));

    assertThat(ex.getMessage(), containsString("Only one of"));
  }

  @Test
  public void testKTooLargeRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=10001') AS v"));

    assertThat(ex.getMessage(), containsString("k must be between 1 and 10000"));
  }

  @Test
  public void testKZeroRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=0') AS v"));

    assertThat(ex.getMessage(), containsString("k must be between 1 and 10000"));
  }

  @Test
  public void testUnknownOptionKeyRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=5,method.ef_search=100') AS v"));

    assertThat(ex.getMessage(), containsString("Unknown option key"));
  }

  @Test
  public void testEmptyVectorRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("must not be empty"));
  }

  @Test
  public void testInvalidFieldNameRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', "
                        + "field='field\\\"injection', vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid field name"));
  }

  @Test
  public void testMissingRequiredOptionRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='') AS v"));

    assertThat(ex.getMessage(), containsString("Missing required option"));
  }

  // ── Sort restriction validation ─────────────────────────────────────────

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
  public void testOrderByNonScoreFieldRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5') AS v "
                        + "ORDER BY v.firstname ASC "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("unsupported sort expression"));
  }

  @Test
  public void testOrderByScoreAscRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5') AS v "
                        + "ORDER BY v._score ASC "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("_score ASC is not supported"));
  }

  // ── Compound predicate and radial + WHERE ───────────────────────────────

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

  // ── LIMIT validation ───────────────────────────────────────────────────

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
}
