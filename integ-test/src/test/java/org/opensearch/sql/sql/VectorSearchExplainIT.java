/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Explain-plan integration tests for vectorSearch SQL table function. These tests verify DSL
 * push-down shape via _explain. They do NOT require the k-NN plugin since _explain only parses and
 * plans the query without executing it against a knn index.
 */
public class VectorSearchExplainIT extends SQLIntegTestCase {

  // Matches WrapperQueryBuilder's base64 payload in explain JSON. The explain output escapes
  // quotes as \", so the regex tolerates both \" and " forms around the query key/value.
  private static final Pattern WRAPPER_PAYLOAD =
      Pattern.compile("\\\\?\"query\\\\?\":\\\\?\"([A-Za-z0-9+/=]+)\\\\?\"");
  // Anchored on the surrounding `sourceBuilder=...`, `pitId=` tokens in OpenSearchRequest's
  // toString() output. Test-only coupling: if that request-string format changes (token renamed,
  // pitId removed), this helper breaks even when the DSL shape is still correct. Update the regex
  // anchors if that happens.
  private static final Pattern SOURCE_BUILDER_JSON =
      Pattern.compile("sourceBuilder=(\\{.*?\\}), pitId=", Pattern.DOTALL);

  /** Decodes every base64-encoded wrapper payload in the explain output into its knn JSON. */
  private static List<String> decodeWrapperKnnJsons(String explain) {
    List<String> payloads = new ArrayList<>();
    Matcher m = WRAPPER_PAYLOAD.matcher(explain);
    while (m.find()) {
      payloads.add(new String(Base64.getDecoder().decode(m.group(1)), StandardCharsets.UTF_8));
    }
    return payloads;
  }

  /** Returns the single wrapper knn JSON, asserting exactly one is present. */
  private static String decodeSoleKnnJson(String explain) {
    List<String> payloads = decodeWrapperKnnJsons(explain);
    assertEquals(
        "Expected exactly one wrapper query payload in explain:\n" + explain, 1, payloads.size());
    return payloads.get(0);
  }

  /** Extracts and unescapes the sourceBuilder JSON embedded in the explain request string. */
  private static String extractSourceBuilderJson(String explain) {
    Matcher m = SOURCE_BUILDER_JSON.matcher(explain);
    assertTrue("Explain should contain sourceBuilder JSON:\n" + explain, m.find());
    return m.group(1).replace("\\\"", "\"");
  }

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

    assertTrue(
        "Explain should contain track_scores:\n" + explain, explain.contains("track_scores"));

    // Top-k without WHERE should have the knn at the root, not wrapped in an outer bool.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Top-k without WHERE should not wrap knn in an outer bool:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue(
        "knn JSON should contain the vector values:\n" + knnJson,
        knnJson.contains("[1.0,2.0,3.0]"));
    assertTrue("knn JSON should contain k=5:\n" + knnJson, knnJson.contains("\"k\":5"));
    assertFalse(
        "Top-k without WHERE should not embed a filter:\n" + knnJson, knnJson.contains("filter"));
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

    // Radial without WHERE should have the knn at the root, not wrapped in an outer bool.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Radial without WHERE should not wrap knn in an outer bool:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue(
        "knn JSON should contain the vector values:\n" + knnJson, knnJson.contains("[1.0,2.0]"));
    assertTrue(
        "knn JSON should contain max_distance=10.5:\n" + knnJson,
        knnJson.contains("\"max_distance\":10.5"));
    assertFalse(
        "Radial without WHERE should not embed a filter:\n" + knnJson, knnJson.contains("filter"));
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

    // Radial without WHERE should have the knn at the root, not wrapped in an outer bool.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Radial without WHERE should not wrap knn in an outer bool:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue(
        "knn JSON should contain the vector values:\n" + knnJson, knnJson.contains("[1.0,2.0]"));
    assertTrue(
        "knn JSON should contain min_score=0.8:\n" + knnJson,
        knnJson.contains("\"min_score\":0.8"));
    assertFalse(
        "Radial without WHERE should not embed a filter:\n" + knnJson, knnJson.contains("filter"));
  }

  // ── Default (EFFICIENT) pre-filter DSL shape ────────────────────────

  @Test
  public void testExplainDefaultFilterProducesKnnWithFilter() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 10");

    // Default (EFFICIENT) shape: WHERE is embedded inside knn.filter, the knn JSON is base64-
    // encoded inside a WrapperQueryBuilder, and there is no outer bool/must wrapping.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Default EFFICIENT mode should not produce bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertFalse(
        "Default EFFICIENT mode should not contain must clause:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue("knn JSON should contain k=10:\n" + knnJson, knnJson.contains("\"k\":10"));
    assertTrue(
        "Default EFFICIENT mode must embed filter inside knn:\n" + knnJson,
        knnJson.contains("filter"));
    assertTrue(
        "Default EFFICIENT mode must embed the WHERE predicate inside knn:\n" + knnJson,
        knnJson.contains("state"));
  }

  @Test
  public void testExplainDefaultCompoundPredicateProducesKnnWithFilter() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10') AS v "
                + "WHERE v.state = 'TX' AND v.age > 30 "
                + "LIMIT 10");

    // Compound default-mode WHERE must also route through knn.filter: no outer bool/must, and
    // both predicate fields embedded inside the knn payload.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Default EFFICIENT mode should not produce bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertFalse(
        "Default EFFICIENT mode should not contain must clause:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue("knn JSON should contain k=10:\n" + knnJson, knnJson.contains("\"k\":10"));
    assertTrue(
        "Compound default EFFICIENT must embed filter inside knn:\n" + knnJson,
        knnJson.contains("filter"));
    assertTrue(
        "Compound default EFFICIENT must embed the state predicate inside knn:\n" + knnJson,
        knnJson.contains("state"));
    assertTrue(
        "Compound default EFFICIENT must embed the age predicate inside knn:\n" + knnJson,
        knnJson.contains("age"));
  }

  @Test
  public void testExplainDefaultRadialWithWhereProducesKnnWithFilter() throws IOException {
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0]', option='max_distance=10.5') AS v "
                + "WHERE v.state = 'TX' "
                + "LIMIT 100");

    // Radial + default WHERE must also use the EFFICIENT shape: no outer bool/must, radial
    // parameters preserved inside the knn payload, and the WHERE predicate embedded alongside.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Default EFFICIENT mode should not produce bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertFalse(
        "Default EFFICIENT mode should not contain must clause:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertTrue(
        "knn JSON should contain max_distance=10.5:\n" + knnJson,
        knnJson.contains("\"max_distance\":10.5"));
    assertTrue(
        "Radial default EFFICIENT must embed filter inside knn:\n" + knnJson,
        knnJson.contains("filter"));
    assertTrue(
        "Radial default EFFICIENT must embed the WHERE predicate inside knn:\n" + knnJson,
        knnJson.contains("state"));
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

    // Explicit filter_type=post must produce the same bool.must=[knn]/bool.filter=[term] shape as
    // the default, and the WHERE predicate must NOT leak into the knn payload (that would be
    // efficient mode). This is the key false-positive guard: substring-only checks would pass for
    // efficient mode too.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertTrue(
        "Explain should contain bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertTrue(
        "Explain should contain must:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));
    assertTrue(
        "Explain should contain filter:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"filter\""));
    assertTrue(
        "Explain should contain the outer state predicate:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"state.keyword\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertTrue(
        "knn JSON should target the embedding field:\n" + knnJson,
        knnJson.contains("\"embedding\""));
    assertFalse(
        "filter_type=post must not embed the WHERE predicate inside knn:\n" + knnJson,
        knnJson.contains("state"));
    assertFalse(
        "filter_type=post must not embed a filter inside knn:\n" + knnJson,
        knnJson.contains("filter"));
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
    // so we verify structure by: (1) no bool/must in plaintext (that would be post-filter shape),
    // (2) decode the base64 payload to confirm the filter and predicate field are embedded inside
    // the knn query.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Efficient mode should not produce bool query (that is post-filter shape):\n"
            + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertFalse(
        "Efficient mode should not contain must clause:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));

    String knnJson = decodeSoleKnnJson(explain);
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

    // Same efficient-mode shape guarantee as testExplainFilterTypeEfficientProducesKnnWithFilter,
    // with an added ORDER BY _score DESC: no outer bool/must, and the WHERE predicate must be
    // embedded inside the knn payload (efficient filtering, not post-filter).
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertFalse(
        "Efficient mode should not produce bool query (that is post-filter shape):\n"
            + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertFalse(
        "Efficient mode should not contain must clause:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));

    String knnJson = decodeSoleKnnJson(explain);
    assertTrue(
        "Efficient mode knn JSON should contain filter:\n" + knnJson, knnJson.contains("filter"));
    assertTrue(
        "Efficient mode knn JSON should contain the WHERE predicate field:\n" + knnJson,
        knnJson.contains("state"));
  }

  // ── BETWEEN / NOT IN pushdown regression guards ─────────────────────
  // These tests lock in the DSL shape currently produced for BETWEEN and NOT IN predicates
  // when pushed down through vectorSearch(). They exist to catch silent regressions where a
  // change in the v2 FilterQueryBuilder pipeline would fall back to a serialized script query
  // instead of the native range/bool shape the cluster can index-accelerate.

  @Test
  public void testBetweenPushesAsRange() throws IOException {
    // Pin filter_type=post to keep the regression guard aimed at the post-filter serialization
    // path: these assertions lock in the outer bool/must/filter shape that only appears when
    // WHERE is applied alongside knn rather than embedded under knn.filter.
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10,filter_type=post') AS v "
                + "WHERE v.balance BETWEEN 50 AND 200 "
                + "LIMIT 10");

    // BETWEEN is desugared by the analyzer into AND(>=, <=), which FilterQueryBuilder renders as
    // two range clauses combined under a bool. The goal here is regression lock-in: ensure the
    // pushed filter is native range DSL, not a serialized script query.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertTrue(
        "Explain should contain bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertTrue(
        "Explain should contain must clause (knn in scoring context):\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));
    assertTrue(
        "Explain should contain filter clause (WHERE in non-scoring context):\n"
            + sourceBuilderJson,
        sourceBuilderJson.contains("\"filter\""));
    assertTrue(
        "BETWEEN should push as native range DSL:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"range\""));
    assertTrue(
        "Range should target balance field:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"balance\""));
    // RangeQueryBuilder serializes inclusive bounds as from/to + include_lower/include_upper. Lock
    // both the lower bound (50) and upper bound (200) are present in the pushed DSL.
    assertTrue(
        "Range should contain lower bound 50:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"from\" : 50") || sourceBuilderJson.contains("\"from\":50"));
    assertTrue(
        "Range should contain upper bound 200:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"to\" : 200") || sourceBuilderJson.contains("\"to\":200"));
    // Script-query fallback sentinel: the CompoundedScriptEngine lang marker must NOT appear when
    // BETWEEN is pushed down natively.
    assertFalse(
        "BETWEEN must not fall back to a serialized script query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"script\""));

    // POST-filter mode (default): the WHERE predicate must live OUTSIDE the knn payload.
    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertFalse(
        "Post-filter mode must not embed the balance predicate inside knn:\n" + knnJson,
        knnJson.contains("balance"));
    assertFalse(
        "Post-filter mode must not embed a range inside knn:\n" + knnJson,
        knnJson.contains("range"));
  }

  @Test
  public void testNotInPushesAsMustNotTerms() throws IOException {
    // Pin filter_type=post to keep the regression guard aimed at the post-filter serialization
    // path: these assertions lock in the outer bool/must/filter shape that only appears when
    // WHERE is applied alongside knn rather than embedded under knn.filter.
    String explain =
        explainQuery(
            "SELECT v._id, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', "
                + "vector='[1.0, 2.0, 3.0]', option='k=10,filter_type=post') AS v "
                + "WHERE v.gender NOT IN ('M', 'F') "
                + "LIMIT 10");

    // v2 analyzer desugars `x NOT IN (a, b)` into `NOT(x = a OR x = b)`. FilterQueryBuilder maps
    // NOT to bool.must_not and OR to bool.should, so the pushed DSL is must_not[should[term,term]]
    // rather than a single terms clause. The shape we're locking in is: native bool with must_not
    // on the keyword subfield, *not* a serialized script query.
    String sourceBuilderJson = extractSourceBuilderJson(explain);
    assertTrue(
        "Explain should contain bool query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"bool\""));
    assertTrue(
        "Explain should contain must clause (knn in scoring context):\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must\""));
    assertTrue(
        "Explain should contain filter clause (WHERE in non-scoring context):\n"
            + sourceBuilderJson,
        sourceBuilderJson.contains("\"filter\""));
    assertTrue(
        "NOT IN should push as bool.must_not:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"must_not\""));
    // OR-of-equals desugaring means the two literals land in a bool.should of term clauses.
    assertTrue(
        "NOT IN should contain should clause for OR-of-equals desugaring:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"should\""));
    assertTrue(
        "NOT IN should produce term clauses for each literal:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"term\""));
    // Terms target the keyword subfield of gender (text field with .keyword multi-field).
    assertTrue(
        "NOT IN term clauses should target gender.keyword:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"gender.keyword\""));
    // Both literals must be present in the pushed DSL.
    assertTrue(
        "NOT IN should contain the 'M' literal:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"M\""));
    assertTrue(
        "NOT IN should contain the 'F' literal:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"F\""));
    // Script-query fallback sentinel: native pushdown must not degrade to a serialized script.
    assertFalse(
        "NOT IN must not fall back to a serialized script query:\n" + sourceBuilderJson,
        sourceBuilderJson.contains("\"script\""));

    // POST-filter mode (default): the WHERE predicate must live OUTSIDE the knn payload.
    String knnJson = decodeSoleKnnJson(explain);
    assertTrue("knn JSON should contain knn key:\n" + knnJson, knnJson.contains("\"knn\""));
    assertFalse(
        "Post-filter mode must not embed the gender predicate inside knn:\n" + knnJson,
        knnJson.contains("gender"));
    assertFalse(
        "Post-filter mode must not embed must_not inside knn:\n" + knnJson,
        knnJson.contains("must_not"));
  }
}
