/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
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
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the partial-result path on a text/keyword mapping conflict. A field mapped
 * as {@code keyword} in one index and {@code text} (without a {@code .keyword} sub-field) in
 * another collapses to text-without-keyword across the wildcard pattern. Aggregating that field is
 * possible but expensive: since #5646 it pushes down as a per-document {@code _source} script that
 * reads every document.
 *
 * <p>When {@code plugins.query.partial_result.on_mapping_conflict.enabled} is on, the aggregation
 * is instead pushed down natively over just the aggregatable (keyword) index subset — far faster,
 * but <b>incomplete</b> — and the response carries a {@code PARTIAL_RESULT} warning naming the
 * excluded indices. When off, the complete (slow) result is returned with no warning.
 */
public class CalcitePartialResultOnMappingConflictIT extends PPLIntegTestCase {

  private static final String KEYWORD_INDEX = "partial_conflict_keyword";
  private static final String TEXT_INDEX = "partial_conflict_text";
  private static final String PATTERN = "partial_conflict_*";

  private static final String NESTED_KEYWORD_INDEX = "partial_nested_keyword";
  private static final String NESTED_TEXT_INDEX = "partial_nested_text";
  private static final String NESTED_PATTERN = "partial_nested_*";

  // Truncation fixture: 1 keyword index + 8 bare-text indices, so the excluded list exceeds the
  // warning's spell-out cap and must be summarized as "... and N more".
  private static final String MANY_KEYWORD_INDEX = "partial_many_keyword";
  private static final String MANY_TEXT_PREFIX = "partial_many_text";
  private static final String MANY_PATTERN = "partial_many_*";
  private static final int MANY_TEXT_COUNT = 8;

  // Priority-ladder fixture: one keyword index vs two text-with-.keyword indices. Keyword is
  // outnumbered, so a count-based majority would keep the text-with-.keyword group; the
  // deterministic keyword-first rule must keep the single keyword index instead.
  private static final String PRIORITY_KEYWORD_INDEX = "partial_priority_keyword";
  private static final String PRIORITY_TEXTKW_INDEX_1 = "partial_priority_textkw1";
  private static final String PRIORITY_TEXTKW_INDEX_2 = "partial_priority_textkw2";
  private static final String PRIORITY_PATTERN = "partial_priority_*";

  // Multi-field expression fixture: two fields, both keyword in one index and both bare text in
  // another. A group key like concat(city, region) must trace to BOTH fields and keep only the
  // index where both are aggregatable.
  private static final String MULTI_KEYWORD_INDEX = "partial_multi_keyword";
  private static final String MULTI_TEXT_INDEX = "partial_multi_text";
  private static final String MULTI_PATTERN = "partial_multi_*";

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

    // Priority ladder: 1 keyword index vs 2 text-with-.keyword indices (keyword outnumbered 2:1).
    if (!isIndexExist(client(), PRIORITY_KEYWORD_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), PRIORITY_KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + PRIORITY_KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"env\":\"prod\"}\n" + "{\"index\":{}}\n{\"env\":\"dev\"}\n");
      performRequest(client(), bulk);
    }
    String textKwMapping =
        "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"text\",\"fields\":"
            + "{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}";
    for (String idx : new String[] {PRIORITY_TEXTKW_INDEX_1, PRIORITY_TEXTKW_INDEX_2}) {
      if (!isIndexExist(client(), idx)) {
        createIndexByRestClient(client(), idx, textKwMapping);
        Request bulk = new Request("POST", "/" + idx + "/_bulk?refresh=true");
        bulk.setJsonEntity(
            "{\"index\":{}}\n{\"env\":\"prod\"}\n" + "{\"index\":{}}\n{\"env\":\"stage\"}\n");
        performRequest(client(), bulk);
      }
    }

    // Truncation: 1 keyword + many bare-text indices, so the excluded list exceeds the warning cap.
    if (!isIndexExist(client(), MANY_KEYWORD_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), MANY_KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + MANY_KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity("{\"index\":{}}\n{\"env\":\"prod\"}\n");
      performRequest(client(), bulk);
    }
    // Multi-field expression fixture: both fields keyword in one index, both bare text in another.
    if (!isIndexExist(client(), MULTI_KEYWORD_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"city\":{\"type\":\"keyword\"},"
              + "\"region\":{\"type\":\"keyword\"}}}}";
      createIndexByRestClient(client(), MULTI_KEYWORD_INDEX, mapping);
      Request bulk = new Request("POST", "/" + MULTI_KEYWORD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"city\":\"nyc\",\"region\":\"us\"}\n"
              + "{\"index\":{}}\n{\"city\":\"nyc\",\"region\":\"us\"}\n"
              + "{\"index\":{}}\n{\"city\":\"sf\",\"region\":\"us\"}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), MULTI_TEXT_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"city\":{\"type\":\"text\"},"
              + "\"region\":{\"type\":\"text\"}}}}";
      createIndexByRestClient(client(), MULTI_TEXT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + MULTI_TEXT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"city\":\"la\",\"region\":\"us\"}\n"
              + "{\"index\":{}}\n{\"city\":\"sea\",\"region\":\"us\"}\n");
      performRequest(client(), bulk);
    }

    String bareTextMapping =
        "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"text\"}}}}";
    for (int i = 1; i <= MANY_TEXT_COUNT; i++) {
      String idx = MANY_TEXT_PREFIX + i;
      if (!isIndexExist(client(), idx)) {
        createIndexByRestClient(client(), idx, bareTextMapping);
        Request bulk = new Request("POST", "/" + idx + "/_bulk?refresh=true");
        bulk.setJsonEntity("{\"index\":{}}\n{\"env\":\"prod\"}\n");
        performRequest(client(), bulk);
      }
    }
  }

  @Test
  public void partialResultOffReturnsCompleteResultWithoutWarning() throws IOException {
    setPartialResult(false);
    // Since #5646 the collapsed text group key pushes down as a per-document _source script, so the
    // complete answer is returned (slowly) rather than failing. Every index contributes: the
    // keyword
    // index (prod=2, dev=1) plus the text index (prod=1, qa=1).
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env | sort env", PATTERN));
    verifyDataRows(result, rows(1, "dev"), rows(1, "qa"), rows(3, "prod"));
    assertTrue("a complete result carries no partial-result warning", !result.has("warnings"));
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
  public void partialResultOnHandlesEvalDerivedGroupKey() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // The group key is an expression over the conflicting field (upper(env)), not the bare field.
    // Partitioning traces it back to env, so the keyword index is kept and the text index excluded
    // just as for a bare group key. Only the keyword index contributes: PROD=2, DEV=1.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval g = upper(env) | stats count() by g | sort g", PATTERN));
    verifyDataRows(result, rows(1, "DEV"), rows(2, "PROD"));

    assertTrue("response should carry a warnings array", result.has("warnings"));
    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning should name the underlying field the expression reads",
        warning.getString("detail").contains("env"));
    assertTrue(
        "warning should name the excluded text index",
        warning.getString("detail").contains(TEXT_INDEX));
  }

  @Test
  public void partialResultOnHandlesMultiFieldExpressionGroupKey() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // The group key reads two fields (concat(city, region)); partitioning must trace it to BOTH and
    // keep only the index where both are aggregatable. Keyword index: nycus=2, sfus=1; text
    // excluded.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval g = concat(city, region) | stats count() by g | sort g",
                MULTI_PATTERN));
    verifyDataRows(result, rows(2, "nycus"), rows(1, "sfus"));

    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning should name both underlying fields the expression reads",
        warning.getString("detail").contains("city")
            && warning.getString("detail").contains("region"));
    assertTrue(
        "warning should name the excluded text index",
        warning.getString("detail").contains(MULTI_TEXT_INDEX));
  }

  @Test
  public void partialResultKeepsKeywordGroupEvenWhenOutnumbered() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // Keyword is outnumbered 2:1 by text-with-.keyword indices. The deterministic keyword-first
    // rule keeps the single keyword index (prod:1, dev:1) and excludes both text-with-.keyword
    // indices -- a count-based majority would have kept the text group instead.
    JSONObject result =
        executeQuery(
            String.format("source=%s | stats count() by env | sort env", PRIORITY_PATTERN));
    verifyDataRows(result, rows(1, "dev"), rows(1, "prod"));

    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "both text-with-keyword indices should be excluded",
        warning.getString("detail").contains(PRIORITY_TEXTKW_INDEX_1)
            && warning.getString("detail").contains(PRIORITY_TEXTKW_INDEX_2));
  }

  @Test
  public void partialResultWarningTruncatesLargeExcludedList() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env", MANY_PATTERN));

    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    // 8 bare-text indices excluded; the message reports the exact count...
    assertTrue(
        "message should report the full excluded count",
        warning.getString("message").contains("8 of 9"));
    // ...but the detail spells out only a few and summarizes the rest.
    String detail = warning.getString("detail");
    assertTrue("detail should summarize the remainder", detail.contains("and 3 more"));
    assertTrue(
        "detail should not list every excluded index", !detail.contains(MANY_TEXT_PREFIX + "8"));
  }

  @Test
  public void partialResultRefusedForCsvFormat() throws IOException {
    setPartialResult(true);
    // CSV has no warnings channel, so partial mode must NOT silently drop the text index -- there
    // would be no way to tell the caller the numbers are undercounted. It falls through to the
    // normal (complete) path instead, so the text index's rows are still counted.
    String csv =
        executeCsvQuery(
            String.format("source=%s | stats count() by env | sort env", PATTERN), false);
    // qa exists only in the excluded text index: its presence proves nothing was dropped.
    assertTrue(
        "CSV must return the complete result, including the text index: " + csv,
        csv.contains("qa"));
  }

  private void setPartialResult(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.PARTIAL_RESULT_ON_MAPPING_CONFLICT.getKeyValue(),
            Boolean.toString(enabled)));
  }

  private void setPitContextLimit(String value) throws IOException {
    updateClusterSettings(new ClusterSetting("transient", "search.max_open_pit_context", value));
  }
}
