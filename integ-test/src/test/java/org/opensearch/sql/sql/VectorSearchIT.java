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

  // ── OFFSET / WHERE _score / filter_type=efficient script rejection ───

  @Test
  public void testOffsetRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5') AS v "
                        + "LIMIT 5 OFFSET 2"));

    assertThat(ex.getMessage(), containsString("OFFSET is not supported on vectorSearch()"));
    assertThat(ex.getMessage(), containsString("LIMIT only"));
  }

  @Test
  public void testScoreInWhereRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5') AS v "
                        + "WHERE v._score > 0.5 "
                        + "LIMIT 5"));

    assertThat(ex.getMessage(), containsString("WHERE on _score is not supported"));
    assertThat(ex.getMessage(), containsString("min_score"));
  }

  @Test
  public void testOrderByScoreDescLimitOffsetRejected() throws IOException {
    // The natural user shape pairs sort with pagination: ORDER BY _score DESC LIMIT N OFFSET M.
    // The planner's pushDownSort() path can collapse the sort+limit into a top-k size, so OFFSET
    // must still be rejected by pushDownLimit when the combined form is used. Without this guard
    // the parent builder would push `from: <offset>` and silently shift the top-k window.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5') AS v "
                        + "ORDER BY v._score DESC "
                        + "LIMIT 5 OFFSET 2"));

    assertThat(ex.getMessage(), containsString("OFFSET is not supported on vectorSearch()"));
  }

  @Test
  public void testEfficientModeRejectsScriptPredicate() throws IOException {
    // WHERE age + 1 > 30 compiles to a ScriptQueryBuilder under the hood because the outer >
    // is applied to an arithmetic expression, not a direct field reference. Efficient mode
    // cannot embed script queries under knn.filter, so this must be rejected up front with a
    // clear remediation hint instead of a cluster-side failure.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', "
                        + "vector='[1.0, 2.0]', option='k=5,filter_type=efficient') AS v "
                        + "WHERE v.age + 1 > 30 "
                        + "LIMIT 5"));

    assertThat(
        ex.getMessage(), containsString("vectorSearch WHERE pre-filtering does not support"));
    assertThat(ex.getMessage(), containsString("script queries"));
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

    // Lock in the full user-facing sentence, not just loose substrings. The exact wording is
    // part of the contract and regressions should fail loudly rather than keep passing on a
    // subtly reworded message.
    assertThat(
        ex.getMessage(),
        containsString(
            "vectorSearch() requires the k-NN plugin, which is not installed on this cluster."));
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

    // Assert the scan-operator name, not just "wrapper": the name confirms the plan reached
    // the vectorSearch scan builder rather than some other scan shape.
    assertThat(explain, containsString("VectorSearchIndexScan"));
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
  public void testWildcardTableRejectedWithDedicatedMessage() throws IOException {
    // Wildcards in a table name fan out to multiple indices, which vectorSearch() does not
    // support (top-k semantics, dimension checks, and embedded filter JSON are not defined
    // across heterogeneous shards). Surface a dedicated user-facing error instead of the
    // generic "must contain only alphanumeric..." fallback.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='sql_vector_*', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
    assertThat(ex.getMessage(), containsString("wildcards"));
    assertThat(ex.getMessage(), containsString("single concrete index"));
  }

  @Test
  public void testMultiTargetTableRejectedWithDedicatedMessage() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='idx_a,idx_b', field='f', "
                        + "vector='[1.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid table name"));
    assertThat(ex.getMessage(), containsString("multi-target"));
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
  // vectorSearch() exposes synthetic v._id and v._score columns. A user mapping property of the
  // same name would collide on the response tuple key. OpenSearch blocks _id at mapping time;
  // _score is not blocked, so VectorSearchIndex rejects it at scan-build time.

  @Test
  public void testUserMappingWithIdFieldIsRejectedByOpenSearch() throws IOException {
    // Locks in OpenSearch's rejection of a user property named _id: without it, v._id could
    // collide with a user field at response time. The exact error message belongs to OpenSearch.
    String indexName = "vs_collision_id";
    deleteIndexIfExists(indexName);

    Request createIndex = new Request("PUT", "/" + indexName);
    createIndex.setJsonEntity("{\"mappings\":{\"properties\":{\"_id\":{\"type\":\"keyword\"}}}}");

    expectThrows(ResponseException.class, () -> client().performRequest(createIndex));
  }

  @Test
  public void testVectorSearchAgainstIndexWithScoreFieldRejects() throws IOException {
    // _explain exercises planning (where the guard runs) without needing the k-NN plugin.
    String indexName = "vs_collision_score";
    deleteIndexIfExists(indexName);

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

    assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
    assertThat(ex.getMessage(), containsString("_score"));
    assertThat(ex.getMessage(), containsString("collides"));
  }

  @Test
  public void testSemicolonSeparatorInVectorRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0;2.0]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("vector="));
    assertThat(ex.getMessage(), containsString("comma-separated"));
  }

  @Test
  public void testNegativeMinScoreRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='min_score=-0.5') AS v"));

    assertThat(ex.getMessage(), containsString("min_score"));
    assertThat(ex.getMessage(), containsString("non-negative"));
  }

  @Test
  public void testNegativeMaxDistanceRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0]', option='max_distance=-1.0') AS v"));

    assertThat(ex.getMessage(), containsString("max_distance"));
    assertThat(ex.getMessage(), containsString("non-negative"));
  }

  @Test
  public void testTrailingCommaInVectorRejected() throws IOException {
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT v._id FROM vectorSearch(table='t', field='f', "
                        + "vector='[1.0,2.0,]', option='k=5') AS v"));

    assertThat(ex.getMessage(), containsString("Invalid vector component"));
    assertThat(ex.getMessage(), containsString("trailing or consecutive commas"));
  }

  // ── Alias with multiple backing indices ───────────────────────────────
  // vectorSearch() accepts an alias as `table=`. When the alias points at multiple backing
  // indices, planning must accept the alias string instead of treating it as a wildcard or
  // multi-target. Execution correctness over compatible knn_vector mappings is a separate
  // concern covered by k-NN-enabled tests/follow-up; these tests lock in planning acceptance
  // only, via _explain on the default no-kNN cluster.

  @Test
  public void testExplainOverAliasWithMultipleBackingIndices() throws IOException {
    // Create two indices with identical keyword mappings (no knn_vector, since the plugin is
    // not installed) and a shared alias. We only assert the planner accepts the alias; whether
    // k-NN accepts the alias at execution is a separate concern tested on a k-NN-enabled
    // cluster.
    // Randomized names so a stale alias/index left by an aborted prior run of this class does
    // not shadow a fresh setup, which is a concrete risk on local reruns.
    String suffix = java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String idx1 = "vector_alias_backing_1_" + suffix;
    String idx2 = "vector_alias_backing_2_" + suffix;
    String alias = "vector_alias_combined_" + suffix;
    try {
      createSimpleIndex(idx1);
      createSimpleIndex(idx2);
      addToAlias(idx1, alias);
      addToAlias(idx2, alias);

      String explain =
          explainQuery(
              "SELECT v._id FROM vectorSearch(table='"
                  + alias
                  + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v");

      assertThat(explain, containsString("VectorSearchIndexScan"));
      assertThat(explain, containsString(alias));
    } finally {
      // Deleting the backing indices removes the alias automatically, but delete the alias
      // first for robustness against partial setup failures.
      deleteAliasIfExists(alias);
      deleteIndexIfExists(idx1);
      deleteIndexIfExists(idx2);
    }
  }

  private void createSimpleIndex(String indexName) throws IOException {
    Request create = new Request("PUT", "/" + indexName);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"state\":{\"type\":\"keyword\"}}}}");
    client().performRequest(create);
  }

  private void addToAlias(String indexName, String aliasName) throws IOException {
    Request req = new Request("POST", "/_aliases");
    req.setJsonEntity(
        "{\"actions\":[{\"add\":{\"index\":\""
            + indexName
            + "\",\"alias\":\""
            + aliasName
            + "\"}}]}");
    client().performRequest(req);
  }

  private void deleteIndexIfExists(String indexName) {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (IOException ignored) {
      // Index does not exist, which is fine.
    }
  }

  private void deleteAliasIfExists(String aliasName) {
    try {
      client().performRequest(new Request("DELETE", "/_all/_alias/" + aliasName));
    } catch (IOException ignored) {
      // Alias does not exist, which is fine.
    }
  }
}
