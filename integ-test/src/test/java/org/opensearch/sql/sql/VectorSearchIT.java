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
 * Integration tests for vectorSearch SQL table function — validation and error paths. These tests
 * verify that invalid inputs are rejected with clear error messages. Explain-plan DSL shape tests
 * live in {@link VectorSearchExplainIT}.
 */
public class VectorSearchIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  private static final String TEST_INDEX = TestsConstants.TEST_INDEX_ACCOUNT;

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

  @Test
  public void testRadialWithoutLimitRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='max_distance=10.5') AS v"));

    assertThat(ex.getMessage(), containsString("LIMIT is required for radial vector search"));
  }

  // ── Sort restriction validation ─────────────────────────────────────────

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

  // ── filter_type validation ────────────────────────────────────────────

  @Test
  public void testFilterTypeEfficientWithoutWhereRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5,filter_type=efficient') AS v "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("filter_type requires a pushdownable WHERE clause"));
  }

  @Test
  public void testFilterTypePostWithoutWhereRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5,filter_type=post') AS v "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("filter_type requires a pushdownable WHERE clause"));
  }

  @Test
  public void testInvalidFilterTypeRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='k=5,filter_type=bogus') AS v"));

    assertThat(ex.getMessage(), containsString("filter_type must be one of"));
  }

  @Test
  public void testGroupByRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v.gender, COUNT(*) FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='f', vector='[1.0]', option='k=5') AS v GROUP BY v.gender"));

    assertThat(
        ex.getMessage(),
        containsString("Aggregations are not supported on vectorSearch() relations"));
  }

  @Test
  public void testBareAggregateRejects() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT COUNT(*) FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='f', vector='[1.0]', option='k=5') AS v"));

    assertThat(
        ex.getMessage(),
        containsString("Aggregations are not supported on vectorSearch() relations"));
  }

  // ── k-NN plugin capability check ──────────────────────────────────────
  // The default integ-test cluster does not have the k-NN plugin installed. Execution-path
  // queries against vectorSearch() should therefore fail with the clear "k-NN plugin missing"
  // error from KnnPluginCapability, while _explain continues to work because the capability
  // probe is deferred to scan open() and does not run during analysis/planning.

  @Test
  public void testExecutionWithoutKnnPluginReturnsCapabilityError() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("k-NN plugin"));
    assertThat(ex.getMessage(), containsString("not installed"));
  }

  @Test
  public void testExplainWithoutKnnPluginStillWorks() throws IOException {
    // _explain only parses and plans the query. It must NOT require the k-NN plugin — the
    // capability probe is intentionally deferred to scan open() so pluginless clusters can
    // still inspect query plans. If this test starts failing with "k-NN plugin not installed",
    // the probe has leaked back into an analysis-time path.
    String explain =
        explainQuery(
            "SELECT v._id FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                + "LIMIT 5");

    assertThat(explain, containsString("wrapper"));
  }
}
