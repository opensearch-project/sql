/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.Request;
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

  // ── Argument shape validation ─────────────────────────────────────────

  @Test
  public void testInvalidTableNameRejected() throws IOException {
    // A slash is outside the SAFE_FIELD_NAME regex and is not a valid OpenSearch index character,
    // so it should be rejected at the SQL layer before any cluster call is attempted.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='idx/evil', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
  }

  @Test
  public void testDuplicateNamedArgRejected() throws IOException {
    // Previously this crashed the server with 500 ArrayIndexOutOfBoundsException. Must now
    // surface as a clean 400 with a user-facing message.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='a', table='b', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Duplicate argument name"));
  }

  @Test
  public void testUnknownNamedArgRejected() throws IOException {
    // A grammar-legal but unknown name must surface as a clean 400 from the resolver.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(bogus='idx', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Unknown argument name"));
  }

  @Test
  public void testPositionalArgRejected() throws IOException {
    // The real shape a user would hit: `vectorSearch('idx', field=..., vector=..., option=...)`.
    // The V2 grammar now accepts this form so the AstBuilder can surface a clean
    // SemanticCheckException instead of letting the request fall back to the legacy SQL engine,
    // which previously returned 200 with zero rows.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch('idx', field='embedding', "
                        + "vector='[1.0, 1.0]', option='k=3') AS v LIMIT 3"));

    assertThat(ex.getMessage(), containsString("requires named arguments"));
  }

  @Test
  public void testCaseInsensitiveDuplicateArgRejected() throws IOException {
    // Argument names are normalized to lower-case, so `table` and `TABLE` must be treated as the
    // same key and rejected as a duplicate rather than silently keeping one of the two values.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='a', TABLE='b', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Duplicate argument name"));
  }

  @Test
  public void testTableNameAllRejected() throws IOException {
    // `_all` would fan out to every index. The preview contract is a single concrete index or
    // alias, so it must be rejected explicitly rather than allowed to route broadly.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='_all', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
  }

  @Test
  public void testTableNameSingleDotRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='.', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
  }

  @Test
  public void testTableNameDoubleDotRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='..', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
  }

  @Test
  public void testMissingRequiredArgRejected() throws IOException {
    // Omitting a required named argument (here: `field`) must produce a clean 400 rather than a
    // NullPointerException or a legacy-engine fallback.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='a', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("requires 4 arguments"));
  }

  /**
   * Users running FROM vectorSearch(...) without an AS alias previously received an opaque parser
   * error from the legacy SQL engine fallback. The clearer SemanticCheckException from the v2
   * engine must surface to the user instead.
   */
  @Test
  public void testVectorSearchRequiresAlias() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT * FROM vectorSearch("
                        + "table='t', field='f', vector='[1.0]', option='k=5') "
                        + "LIMIT 3"));

    String body = ex.getMessage();
    assertThat(body, containsString("requires a table alias"));
    assertThat(body, containsString("vectorSearch"));
  }

  // Synthetic column collision (metadata vs. user field).
  //
  // v._score and v._id on a vectorSearch() relation are synthetic columns that expose the k-NN
  // similarity score and the document metadata _id. A user-defined stored field of the same name
  // would collide with these synthetic columns in the response tuple. Probe results on
  // OpenSearch 3.6:
  //   - _id: OpenSearch's mapper rejects a property named _id at mapping time, so no collision
  //     is possible. The testUserMappingWithIdFieldIsRejectedByOpenSearch test locks this in.
  //   - _score: OpenSearch accepts a property named _score. Left unchecked the SQL response
  //     layer would fail with an opaque duplicate-key error when the hit's _source contains the
  //     user field and the metadata _score both write to the same tuple key. VectorSearchIndex
  //     therefore rejects the mapping at scan-build time with a clear SQL error; the
  //     testVectorSearchAgainstIndexWithScoreFieldRejects test exercises that guard (works via
  //     _explain so no k-NN plugin is needed).

  @Test
  public void testUserMappingWithIdFieldIsRejectedByOpenSearch() throws IOException {
    String indexName = "vs_collision_id";
    deleteIndexIfExists(indexName);

    // OpenSearch rejects _id as a user mapping property; this guarantees v._id on a
    // vectorSearch() relation always resolves to the document metadata _id. The exact error
    // message belongs to OpenSearch; this test only locks in that the mapping creation fails.
    Request createIndex = new Request("PUT", "/" + indexName);
    createIndex.setJsonEntity("{\"mappings\":{\"properties\":{\"_id\":{\"type\":\"keyword\"}}}}");

    expectThrows(ResponseException.class, () -> client().performRequest(createIndex));
  }

  @Test
  public void testVectorSearchAgainstIndexWithScoreFieldRejects() throws IOException {
    String indexName = "vs_collision_score";
    deleteIndexIfExists(indexName);

    // Unlike _id, OpenSearch accepts _score as a user-defined mapping property. Without the
    // VectorSearchIndex guard, a vectorSearch() query against such an index would fail at
    // response time with an opaque duplicate-key error because the stored _score field and the
    // metadata _score both map to the same tuple key. The guard raises a clear SQL error
    // up-front. Using _explain here exercises planning (which runs the guard) without needing
    // the k-NN plugin.
    Request createIndex = new Request("PUT", "/" + indexName);
    createIndex.setJsonEntity("{\"mappings\":{\"properties\":{\"_score\":{\"type\":\"float\"}}}}");
    client().performRequest(createIndex);

    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT v._score FROM vectorSearch(table='"
                        + indexName
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("_score"));
    assertThat(ex.getMessage(), containsString("collides"));
  }

  private void deleteIndexIfExists(String indexName) {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (IOException ignored) {
      // Index does not exist, which is fine.
    }
  }
}
