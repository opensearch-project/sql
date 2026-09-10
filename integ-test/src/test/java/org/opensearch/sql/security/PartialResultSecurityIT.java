/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.common.setting.Settings;

/**
 * Runs the partial-result-on-mapping-conflict path with the security plugin installed. Regression
 * guard for #5739: the warnings-supported gate used to live in Log4j {@code ThreadContext}, which
 * the security plugin's transport interceptor drops on the transport-to-worker handoff, so partial
 * mode silently bailed and returned a complete result with no warning. The {@code
 * integTestWithSecurity} suite never exercised this path -- it only runs {@code
 * org.opensearch.sql.security.*}, and the partial-result IT lives elsewhere -- so only the release
 * distribution's full-suite-with-security caught it. Placing this test in the security package
 * closes that gap.
 */
public class PartialResultSecurityIT extends SecurityTestBase {

  private static final String KEYWORD_INDEX = "partial_sec_keyword";
  private static final String TEXT_INDEX = "partial_sec_text";
  private static final String PATTERN = "partial_sec_*";

  private static final String USER = "partial_sec_user";
  private static final String ROLE = "partial_sec_role";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    if (!initialized) {
      createRoleWithIndexAccess(ROLE, PATTERN);
      createUser(USER, ROLE);
      createConflictIndices();
      initialized = true;
    }
  }

  @After
  public void resetPartialResult() throws IOException {
    setPartialResult(false);
  }

  private void createConflictIndices() throws IOException {
    // env is an aggregatable keyword here...
    if (!isIndexExist(client(), KEYWORD_INDEX)) {
      String mapping = "{\"mappings\":{\"properties\":{\"env\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"env\":\"prod\"}\n"
              + "{\"index\":{}}\n{\"env\":\"prod\"}\n"
              + "{\"index\":{}}\n{\"env\":\"dev\"}\n");
      performRequest(client(), bulk);
    }
    // ...and bare text (no .keyword sub-field) here, so the field collapses to non-aggregatable.
    if (!isIndexExist(client(), TEXT_INDEX)) {
      String mapping = "{\"mappings\":{\"properties\":{\"env\":{\"type\":\"text\"}}}}";
      createIndexByRestClient(client(), TEXT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + TEXT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"env\":\"prod\"}\n" + "{\"index\":{}}\n{\"env\":\"qa\"}\n");
      performRequest(client(), bulk);
    }
  }

  @Test
  public void partialResultWarningSurvivesSecurityHandoff() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | stats count() by env | sort env", PATTERN), USER);

    // Only the aggregatable keyword index contributes (prod=2, dev=1); the text index is excluded.
    verifyDataRows(result, rows(1, "dev"), rows(2, "prod"));

    assertTrue(
        "partial result must carry a warnings channel under security", result.has("warnings"));
    JSONArray warnings = result.getJSONArray("warnings");
    assertEquals(1, warnings.length());
    JSONObject warning = warnings.getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning should name the excluded text index",
        warning.getString("detail").contains(TEXT_INDEX));
  }

  @Test
  public void completeResultCarriesNoWarningWithSecurity() throws IOException {
    setPartialResult(false);
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | stats count() by env | sort env", PATTERN), USER);
    // Every index contributes: keyword (prod=2, dev=1) + text (prod=1, qa=1).
    verifyDataRows(result, rows(1, "dev"), rows(1, "qa"), rows(3, "prod"));
    assertFalse("a complete result carries no warning", result.has("warnings"));
  }

  @Test
  public void perRequestPartialResultFalseOverridesClusterSettingUnderSecurity()
      throws IOException {
    // Cluster setting ON, but the request explicitly opts OUT via partial_result=false. The
    // per-request override must win -> complete result over all indices, no warning. On the buggy
    // code the override lives in Log4j ThreadContext (QueryContext.setPartialResultOverride) and is
    // dropped by the security transport->worker handoff -- the same drop #5739 fixed for
    // warningsSupported but left in place for the override -- so it silently falls back to the ON
    // cluster setting and returns a partial result with a warning.
    setPartialResult(true);
    JSONObject result =
        executeQueryAsUserWithPartialResult(
            String.format("source=%s | stats count() by env | sort env", PATTERN), USER, false);
    // Every index contributes: keyword (prod=2, dev=1) + text (prod=1, qa=1).
    verifyDataRows(result, rows(1, "dev"), rows(1, "qa"), rows(3, "prod"));
    assertFalse(
        "partial_result=false must override the ON cluster setting -> complete result, no warning",
        result.has("warnings"));
  }

  /** Like {@link #executeQueryAsUser}, but also sends the per-request {@code partial_result}. */
  private JSONObject executeQueryAsUserWithPartialResult(
      String query, String username, boolean partialResult) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{ \"query\": \"%s\", \"partial_result\": %s }",
            query,
            Boolean.toString(partialResult)));
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    options.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(options);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  private void setPartialResult(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.PARTIAL_RESULT_ON_MAPPING_CONFLICT.getKeyValue(),
            Boolean.toString(enabled)));
  }
}
