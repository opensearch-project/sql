/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the partial-result fallback on a text/keyword mapping conflict. A field
 * mapped as {@code keyword} in one index and {@code text} (without a {@code .keyword} sub-field) in
 * another collapses to text-without-keyword across the wildcard pattern, which defeats aggregation
 * pushdown and forces a per-shard document scan that opens a Point-In-Time context on every shard.
 *
 * <p>When {@code plugins.calcite.partial_result.on_mapping_conflict.enabled} is on, the aggregation
 * is instead pushed down over just the aggregatable (keyword) index subset — no PIT — and the
 * response carries a {@code PARTIAL_RESULT} warning naming the excluded index.
 */
public class CalcitePartialResultOnMappingConflictIT extends PPLIntegTestCase {

  private static final String KEYWORD_INDEX = "partial_conflict_keyword";
  private static final String TEXT_INDEX = "partial_conflict_text";
  private static final String PATTERN = "partial_conflict_*";

  private static final String NESTED_KEYWORD_INDEX = "partial_nested_keyword";
  private static final String NESTED_TEXT_INDEX = "partial_nested_text";
  private static final String NESTED_PATTERN = "partial_nested_*";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndices();
  }

  @After
  public void cleanup() throws IOException {
    setPartialResult(false);
    setPitContextLimit(null);
  }

  private void createTestIndices() throws IOException {
    // keyword index: env is aggregatable. Two shards so a scan needs 2 PIT contexts.
    if (!isIndexExist(client(), KEYWORD_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"env\":\"prod\"}\n"
              + "{\"index\":{}}\n{\"env\":\"prod\"}\n"
              + "{\"index\":{}}\n{\"env\":\"dev\"}\n");
      performRequest(client(), bulk);
    }
    // text index (no .keyword sub-field): env is NOT aggregatable -> forces the conflict collapse.
    if (!isIndexExist(client(), TEXT_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"text\"}}}}";
      createIndexByRestClient(client(), TEXT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + TEXT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"env\":\"prod\"}\n" + "{\"index\":{}}\n{\"env\":\"qa\"}\n");
      performRequest(client(), bulk);
    }

    // A nested/dotted field (resource.attributes.env) is stored as an object tree in the mapping,
    // so the partitioning must flatten it to match the bucket field's dotted path. Mirrors the
    // real observability shape (e.g. resource.attributes.applicationid).
    if (!isIndexExist(client(), NESTED_KEYWORD_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"resource\":{\"properties\":{\"attributes\":"
              + "{\"properties\":{\"env\":{\"type\":\"keyword\"}}}}}}}}";
      createIndexByRestClient(client(), NESTED_KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + NESTED_KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"resource\":{\"attributes\":{\"env\":\"prod\"}}}\n"
              + "{\"index\":{}}\n{\"resource\":{\"attributes\":{\"env\":\"prod\"}}}\n"
              + "{\"index\":{}}\n{\"resource\":{\"attributes\":{\"env\":\"dev\"}}}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), NESTED_TEXT_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"resource\":{\"properties\":{\"attributes\":"
              + "{\"properties\":{\"env\":{\"type\":\"text\"}}}}}}}}";
      createIndexByRestClient(client(), NESTED_TEXT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + NESTED_TEXT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"resource\":{\"attributes\":{\"env\":\"prod\"}}}\n"
              + "{\"index\":{}}\n{\"resource\":{\"attributes\":{\"env\":\"qa\"}}}\n");
      performRequest(client(), bulk);
    }
  }

  @Test
  public void partialResultOffFailsWhenPitExhausted() throws IOException {
    setPartialResult(false);
    // Below the shard count of the pattern (4 shards) so the forced scan cannot open its PITs.
    setPitContextLimit("1");
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeQuery(String.format("source=%s | stats count() by env", PATTERN)));
    assertTrue(e.getResponse().getStatusLine().getStatusCode() >= 400);
  }

  @Test
  public void partialResultOnReturnsKeywordSubsetWithWarning() throws IOException {
    setPartialResult(true);
    // Even with the PIT budget crippled, partial mode pushes the aggregation down (size=0), so no
    // PIT is opened and the query succeeds over the aggregatable keyword index only.
    setPitContextLimit("1");
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env | sort env", PATTERN));

    // Only the keyword index contributes: prod=2, dev=1. The text index (prod=1, qa=1) is excluded.
    verifyDataRows(result, rows(1, "dev"), rows(2, "prod"));

    assertTrue("response should carry a warnings array", result.has("warnings"));
    JSONArray warnings = result.getJSONArray("warnings");
    assertEquals(1, warnings.length());
    JSONObject warning = warnings.getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning detail should name the excluded text index",
        warning.getString("detail").contains(TEXT_INDEX));
  }

  @Test
  public void partialResultOffOmitsWarningWhenNoConflict() throws IOException {
    setPartialResult(true);
    setPitContextLimit(null);
    // A single-index aggregatable query has no conflict, so it pushes down normally and no warning
    // is attached even with partial mode enabled.
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env | sort env", KEYWORD_INDEX));
    verifyDataRows(result, rows(1, "dev"), rows(2, "prod"));
    assertTrue("no warning expected on a clean aggregation", !result.has("warnings"));
  }

  @Test
  public void partialResultOnHandlesNestedDottedField() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // The grouped field is a nested/dotted path; the partitioning must flatten the mapping to find
    // it. Only the keyword index contributes: prod=2, dev=1.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() by resource.attributes.env | sort"
                    + " `resource.attributes.env`",
                NESTED_PATTERN));
    verifyDataRows(result, rows(1, "dev"), rows(2, "prod"));

    assertTrue("response should carry a warnings array", result.has("warnings"));
    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning should name the dotted field",
        warning.getString("detail").contains("resource.attributes.env"));
    assertTrue(
        "warning should name the excluded nested-text index",
        warning.getString("detail").contains(NESTED_TEXT_INDEX));
  }

  @Test
  public void partialResultRefusedForCsvFormat() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // CSV has no warnings channel, so partial mode must NOT silently drop the text index. It falls
    // through to the normal path, which still exhausts PIT contexts and errors.
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeCsvQuery(String.format("source=%s | stats count() by env", PATTERN), false));
    assertTrue(e.getResponse().getStatusLine().getStatusCode() >= 400);
  }

  private void setPartialResult(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.CALCITE_PARTIAL_RESULT_ON_MAPPING_CONFLICT.getKeyValue(),
            Boolean.toString(enabled)));
  }

  private void setPitContextLimit(String value) throws IOException {
    updateClusterSettings(new ClusterSetting("transient", "search.max_open_pit_context", value));
  }
}
