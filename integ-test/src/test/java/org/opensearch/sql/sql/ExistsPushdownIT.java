/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Explain-plan integration tests asserting that {@code IS NOT NULL} / {@code IS NULL} predicates
 * push down as native OpenSearch {@code exists} DSL rather than as serialized script queries.
 *
 * <p>Before this change both predicates serialized through the compounded script engine, producing
 * a {@code "script"} clause in the pushdown DSL. After this change the v2 filter builder emits
 * {@code {"exists": {"field": ...}}} directly for {@code IS NOT NULL}, and a {@code bool} query
 * with a single {@code must_not[exists]} child for {@code IS NULL}. This matches what downstream
 * tooling, serverless / AOSS, and the Calcite path already produce.
 */
public class ExistsPushdownIT extends SQLIntegTestCase {

  // Anchored on the surrounding `sourceBuilder=...`, `pitId=` tokens in OpenSearchRequest's
  // toString() output. Test-only coupling: if that request-string format changes (token renamed,
  // pitId removed), this helper breaks even when the DSL shape is still correct. Update the regex
  // anchors if that happens.
  private static final Pattern SOURCE_BUILDER_JSON =
      Pattern.compile("sourceBuilder=(\\{.*?\\}), pitId=", Pattern.DOTALL);

  /** Extracts and unescapes the sourceBuilder JSON embedded in the explain request string. */
  private static String extractSourceBuilderJson(String explain) {
    Matcher m = SOURCE_BUILDER_JSON.matcher(explain);
    assertTrue("Explain should contain sourceBuilder JSON:\n" + explain, m.find());
    return m.group(1).replace("\\\"", "\"");
  }

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  private static final String TEST_INDEX = TestsConstants.TEST_INDEX_ACCOUNT;

  @Test
  public void testIsNotNullPushesDownAsExistsQuery() throws IOException {
    String explain =
        explainQuery("SELECT age FROM " + TEST_INDEX + " WHERE age IS NOT NULL LIMIT 1");
    String sourceBuilder = extractSourceBuilderJson(explain);

    assertTrue(
        "IS NOT NULL should push down as native exists DSL:\n" + sourceBuilder,
        sourceBuilder.contains("\"exists\""));
    assertTrue(
        "IS NOT NULL exists DSL should target the 'age' field:\n" + sourceBuilder,
        sourceBuilder.contains("\"field\":\"age\""));
    assertFalse(
        "IS NOT NULL should not fall through to a script query:\n" + sourceBuilder,
        sourceBuilder.contains("\"script\""));
  }

  @Test
  public void testIsNullPushesDownAsMustNotExistsQuery() throws IOException {
    String explain = explainQuery("SELECT age FROM " + TEST_INDEX + " WHERE age IS NULL LIMIT 1");
    String sourceBuilder = extractSourceBuilderJson(explain);

    assertTrue(
        "IS NULL should push down as bool/must_not[exists] DSL:\n" + sourceBuilder,
        sourceBuilder.contains("\"must_not\""));
    assertTrue(
        "IS NULL should wrap a native exists clause:\n" + sourceBuilder,
        sourceBuilder.contains("\"exists\""));
    assertTrue(
        "IS NULL exists DSL should target the 'age' field:\n" + sourceBuilder,
        sourceBuilder.contains("\"field\":\"age\""));
    assertFalse(
        "IS NULL should not fall through to a script query:\n" + sourceBuilder,
        sourceBuilder.contains("\"script\""));
  }
}
