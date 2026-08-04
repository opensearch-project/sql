/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for the {@code rest} leading command on the Calcite path. This first version
 * ships a single built-in endpoint, {@code /_cluster/health} (a deterministic single-row endpoint
 * that carries no network identifiers and needs no redaction). These tests exercise it end to end
 * and verify the allow-list and per-arg gates.
 */
public class CalcitePPLRestIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testRestClusterHealthSchema() throws IOException {
    JSONObject result = executeQuery("| rest '/_cluster/health' | fields response");
    verifySchema(result, schema("response", "string"));
  }

  @Test
  public void testRestClusterHealthResponseIsValidJson() throws IOException {
    JSONObject result =
        executeQuery("| rest '/_cluster/health' | eval ok = json_valid(response) | fields ok");
    verifyDataRows(result, rows(true));
  }

  @Test
  public void testRestClusterHealthJsonExtract() throws IOException {
    JSONObject result =
        executeQuery(
            "| rest '/_cluster/health' | eval status = json_extract(response, 'status')"
                + " | fields status");
    verifySchema(result, schema("status", "string"));
  }

  @Test
  public void testRestClusterHealthSpath() throws IOException {
    JSONObject result =
        executeQuery(
            "| rest '/_cluster/health' | spath input=response path=status output=status"
                + " | where status = 'green' or status = 'yellow' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestClusterHealthComposesDownstream() throws IOException {
    // The rest row source composes with downstream stats exactly like an index scan.
    JSONObject result = executeQuery("| rest '/_cluster/health' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestClusterHealthLocalArg() throws IOException {
    // local=true reads health from the local node; on a single-node cluster the row is unchanged.
    JSONObject result =
        executeQuery("| rest '/_cluster/health' local='true' | stats count() as cnt");
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testRestRejectsNonAllowListedEndpoint() {
    // /_cat/nodes is not registered in this version; it is refused before any transport call.
    assertRestBadRequest("| rest '/_cat/nodes'", "allow-list");
  }

  @Test
  public void testRestRejectsEmptyEndpoint() {
    assertRestBadRequest("| rest ''", "non-empty path");
  }

  @Test
  public void testRestRejectsDisallowedArg() {
    assertRestBadRequest("| rest '/_cluster/health' h='name'", "does not accept arg");
  }

  @Test
  public void testRestRejectsNegativeCount() {
    assertRestBadRequest("| rest '/_cluster/health' count=-1", "non-negative");
  }

  /**
   * Assert a {@code rest} query is refused as a client error: HTTP 400 (not a 500 system error)
   * with the given substring in the response body. Covers allow-list and bad-argument rejection.
   */
  private void assertRestBadRequest(String query, String expectedSubstring) {
    ResponseException e =
        org.junit.Assert.assertThrows(ResponseException.class, () -> executeQuery(query));
    org.junit.Assert.assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    org.junit.Assert.assertTrue(
        "expected [" + expectedSubstring + "] in response body: " + e.getMessage(),
        e.getMessage().contains(expectedSubstring));
  }
}
