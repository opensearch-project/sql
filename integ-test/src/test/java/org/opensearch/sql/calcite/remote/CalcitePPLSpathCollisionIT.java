/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Behavioural contract for issue #5718 — {@code spath} (and any command that funnels through {@code
 * projectPlusOverriding}) assigning to a name that collides with an existing mapped <b>object</b>
 * field.
 *
 * <p>Expected semantics (issue #5718, preferred option): overriding an object parent shadows the
 * <b>entire</b> {@code <name>.*} subtree. After {@code spath input=body output=log}, every {@code
 * log.<key>} reference reads from the freshly extracted value; stale mapped leaves must never be
 * silently readable. This matches the flat-keyword collision case, which either returns the
 * extracted value or raises a clear error — never a silent per-leaf mix.
 */
public class CalcitePPLSpathCollisionIT extends PPLIntegTestCase {

  private static final String COLLISION_INDEX = "test_spath_collision";
  private static final String DYNAMIC_INDEX = "test_spath_collision_dyn";

  /**
   * Explicit mapping mirroring issue #5718: {@code log} is an object with mapped keyword leaves
   * {@code log.level} / {@code log.src}, while {@code body} holds a JSON string whose {@code level}
   * key collides with the mapped leaf.
   */
  private static final String COLLISION_MAPPING =
      "{\"mappings\": {\"properties\": {"
          + "\"log\": {\"properties\": {"
          + "\"level\": {\"type\": \"keyword\"}, \"src\": {\"type\": \"keyword\"}}},"
          + "\"body\": {\"type\": \"text\"}}}}";

  private static final String COLLISION_DOC =
      "{\"log\": {\"level\": \"MAPPED-DEBUG\", \"src\": \"real-object\"},"
          + " \"body\": \"{\\\"level\\\":\\\"ERROR\\\",\\\"msg\\\":\\\"from json\\\"}\"}";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), COLLISION_INDEX)) {
      TestUtils.createIndexByRestClient(client(), COLLISION_INDEX, COLLISION_MAPPING);
      Request doc = new Request("PUT", "/" + COLLISION_INDEX + "/_doc/1?refresh=true");
      doc.setJsonEntity(COLLISION_DOC);
      client().performRequest(doc);
    }

    // Separate index for the dynamic-mapping stability test: doc 2 dynamically maps `log.msg`,
    // which must not change what doc 1's `log.msg` reads after extraction.
    if (!TestUtils.isIndexExist(client(), DYNAMIC_INDEX)) {
      TestUtils.createIndexByRestClient(client(), DYNAMIC_INDEX, COLLISION_MAPPING);
      Request doc1 = new Request("PUT", "/" + DYNAMIC_INDEX + "/_doc/1?refresh=true");
      doc1.setJsonEntity(COLLISION_DOC);
      client().performRequest(doc1);
      Request doc2 = new Request("PUT", "/" + DYNAMIC_INDEX + "/_doc/2?refresh=true");
      doc2.setJsonEntity(
          "{\"log\": {\"level\": \"X\", \"src\": \"y\", \"msg\": \"DYNAMICALLY-MAPPED\"},"
              + " \"body\": \"{\\\"level\\\":\\\"E2\\\",\\\"msg\\\":\\\"json-2\\\"}\"}");
      client().performRequest(doc2);
    }
  }

  @Test
  public void testCollidingOutputLeafReadsExtractedValue() throws IOException {
    // Issue #5718 core case: log.level must read the extracted ERROR, not the stale mapped
    // MAPPED-DEBUG. The whole log.* subtree reads from the extraction: log.msg exists only in
    // the JSON (-> "from json"), log.src exists only in the stale mapping (-> null).
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log | fields log.level, log.msg, log.src",
                COLLISION_INDEX));
    verifyDataRows(result, rows("ERROR", "from json", null));
  }

  @Test
  public void testCollidingOutputParentReadsExtractedMap() throws IOException {
    // Guard (already true before the fix): the parent reference returns the extracted map.
    JSONObject result =
        executeQuery(
            String.format("source=%s | spath input=body output=log | fields log", COLLISION_INDEX));
    verifyDataRows(result, rows(ImmutableMap.of("level", "ERROR", "msg", "from json")));
  }

  @Test
  public void testCollidingOutputWhereMatchesExtractedValue() throws IOException {
    // Issue #5718 symptom B: filtering on the extracted value must match.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log | where log.level = 'ERROR' | fields"
                    + " log.level",
                COLLISION_INDEX));
    verifyDataRows(result, rows("ERROR"));
  }

  @Test
  public void testCollidingOutputWhereStaleValueMatchesNothing() throws IOException {
    // The stale mapped value is shadowed and must no longer be reachable through log.level.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log | where log.level = 'MAPPED-DEBUG' |"
                    + " fields log.level",
                COLLISION_INDEX));
    verifyDataRows(result);
  }

  @Test
  public void testCollidingOutputStableUnderDynamicMapping() throws IOException {
    // Issue #5718 symptom C: indexing an unrelated document that dynamically maps `log.msg`
    // must not change what the original document's `log.msg` reads. Both rows read from their
    // own extracted JSON.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log | fields log.level, log.msg",
                DYNAMIC_INDEX));
    verifyDataRows(result, rows("ERROR", "from json"), rows("E2", "json-2"));
  }

  @Test
  public void testCollidingOutputThenEvalDottedLeaf() throws IOException {
    // Companion defect uncovered while reproducing #5718: with stale leaves present, a
    // subsequent `eval log.level = ...` fired the override path and dropStructParentsFor
    // removed the freshly extracted map (`Field [log] not found`). Expected: the assignment
    // creates the literal column and the extracted parent survives — same semantics as the
    // non-colliding case guarded by issue #5185.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log | eval `log.level` = 'patched' | fields"
                    + " log, `log.level`",
                COLLISION_INDEX));
    verifyDataRows(result, rows(ImmutableMap.of("level", "ERROR", "msg", "from json"), "patched"));
  }

  @Test
  public void testCollidingOutputPathModeParentReadsExtractedValue() throws IOException {
    // Path mode with a colliding output overrides `log` with the scalar extraction result.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=body output=log path=level | fields log",
                COLLISION_INDEX));
    verifyDataRows(result, rows("ERROR"));
  }

  @Test
  public void testCollidingOutputPathModeLeafIsNotSilentlyReadable() {
    // Path mode: `log` is now a scalar, so `log.level` has nothing to resolve against. It must
    // not silently answer from the stale mapped leaf; a clear error mirrors the flat-keyword
    // collision behaviour described in issue #5718.
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | spath input=body output=log path=level | fields log.level",
                    COLLISION_INDEX)));
  }

  @Test
  public void testScalarEvalOverObjectParentIsNotSilentlyReadable() {
    // Generalisation of #5718 beyond spath: overriding a mapped object parent with a scalar
    // must not leave stale leaves silently readable. `log` is an INTEGER after the eval, so a
    // `log.level` reference raises a clear error instead of returning MAPPED-DEBUG.
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format("source=%s | eval log = 1 | fields log.level", COLLISION_INDEX)));
  }

  @Test
  public void testLiteralDottedColumnSurvivesScalarParentOverride() throws IOException {
    // SPL1 guard (reviewer's case on PR #5351 family): a user-created literal dotted column is
    // an independent field. Overriding its scalar name prefix must NOT remove it — subtree
    // shadowing only applies when the overridden column was an object/map parent.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval `body.x` = 7 | eval body = 'replaced' | fields body, `body.x`",
                COLLISION_INDEX));
    verifyDataRows(result, rows("replaced", 7));
  }
}
