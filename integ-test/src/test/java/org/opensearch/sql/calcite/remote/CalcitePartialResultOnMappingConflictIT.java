/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

  // Non-text-type fixture: the field is an aggregatable integer in one index and bare text in
  // another. The integer index is kept and the text index excluded, rather than the field silently
  // coercing to one type and dropping the other index's docs.
  private static final String NUMTEXT_INT_INDEX = "partial_numtext_int";
  private static final String NUMTEXT_TEXT_INDEX = "partial_numtext_text";
  private static final String NUMTEXT_PATTERN = "partial_numtext_*";

  // No aggregatable index at all: the field is bare text everywhere, so no subset can be kept.
  private static final String ALLTEXT_INDEX_1 = "partial_alltext_one";
  private static final String ALLTEXT_INDEX_2 = "partial_alltext_two";
  private static final String ALLTEXT_PATTERN = "partial_alltext_*";

  // Two-aggregation fixture: env and svc are conflicted in different indices, so grouping by each
  // excludes a different subset. Carries a date field to chart over. The prefix must stay outside
  // MULTI_PATTERN and every other pattern above, or it joins their index sets.
  private static final String TWOAGG_ALL_INDEX = "partial_twoagg_all";
  private static final String TWOAGG_ENVTEXT_INDEX = "partial_twoagg_envtext";
  private static final String TWOAGG_SVCTEXT_INDEX = "partial_twoagg_svctext";
  private static final String TWOAGG_PATTERN = "partial_twoagg_*";

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

    // Non-text-type conflict: integer vs bare text on the same field.
    if (!isIndexExist(client(), NUMTEXT_INT_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"val\":{\"type\":\"integer\"}}}}";
      createIndexByRestClient(client(), NUMTEXT_INT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + NUMTEXT_INT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"val\":7}\n"
              + "{\"index\":{}}\n{\"val\":7}\n"
              + "{\"index\":{}}\n{\"val\":9}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), NUMTEXT_TEXT_INDEX)) {
      String mapping =
          "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
              + "\"mappings\":{\"properties\":{\"val\":{\"type\":\"text\"}}}}";
      createIndexByRestClient(client(), NUMTEXT_TEXT_INDEX, mapping);
      Request bulk = new Request("POST", "/" + NUMTEXT_TEXT_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"val\":\"aa\"}\n" + "{\"index\":{}}\n{\"val\":\"bb\"}\n");
      performRequest(client(), bulk);
    }

    createBareTextIndex(ALLTEXT_INDEX_1, "prod");
    createBareTextIndex(ALLTEXT_INDEX_2, "qa");

    createTwoAggIndex(
        TWOAGG_ALL_INDEX,
        "keyword",
        "keyword",
        pair("prod", "api"),
        pair("prod", "api"),
        pair("dev", "api"));
    createTwoAggIndex(
        TWOAGG_ENVTEXT_INDEX, "text", "keyword", pair("prod", "cart"), pair("qa", "cart"));
    createTwoAggIndex(TWOAGG_SVCTEXT_INDEX, "keyword", "text", pair("stage", "web"));

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
  public void partialResultKeepsNumericExcludesText() throws IOException {
    setPartialResult(true);
    setPitContextLimit("1");
    // The field is integer in one index and bare text in another. The integer index is aggregatable
    // so it is kept and the text index excluded (with a warning), instead of silently dropping one
    // index's docs to a coerced type. The bucket labels materialize correctly (no null).
    JSONObject result =
        executeQuery(
            String.format("source=%s | stats count() as c by val | sort val", NUMTEXT_PATTERN));
    // Only the integer index contributes its two buckets; the text values (aa, bb) are excluded.
    assertEquals(2, result.getJSONArray("datarows").length());
    String body = result.toString();
    assertTrue("excluded text values must not appear: " + body, !body.contains("aa"));
    assertTrue("excluded text values must not appear: " + body, !body.contains("bb"));

    JSONObject warning = result.getJSONArray("warnings").getJSONObject(0);
    assertEquals("PARTIAL_RESULT", warning.getString("type"));
    assertTrue(
        "warning should name the excluded text index",
        warning.getString("detail").contains(NUMTEXT_TEXT_INDEX));
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

  // The two-aggregation queries below all narrow to one shared subset: env is aggregatable in the
  // all-keyword and svc-text indices, svc in the all-keyword and env-text indices, so the union
  // {env, svc} is aggregatable only in the all-keyword index. Every aggregate is narrowed to it, so
  // the response is drawn from one population and carries a single warning -- never the mix of
  // populations and duplicate banners a per-aggregate subset produced.

  /** chart groups a scan twice; both group keys narrow to the same subset. */
  @Test
  public void partialResultNarrowsChartToOneSubset() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(String.format("source=%s | chart count() over ts by env", TWOAGG_PATTERN));

    // union {env, ts}: ts is aggregatable everywhere, env only outside the env-text index, so that
    // one index is excluded and qa (its only env) cannot appear.
    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa");
  }

  /** timechart parses into the same node as chart. */
  @Test
  public void partialResultNarrowsTimechartToOneSubset() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(String.format("source=%s | timechart span=1h count() by env", TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa");
  }

  // The dashboards search path attaches a highlight to every request. On a chart/timechart (which
  // groups a scan more than once) its synthetic _highlight column defeats group-key origin tracing;
  // before the fix that barred partial mode and dropped the warning. Highlight is meaningless once
  // rows collapse to buckets, so it must be ignored and the query must still narrow + warn.

  /** chart with a request-body highlight still narrows to one subset and warns. */
  @Test
  public void partialResultNarrowsChartWithHighlight() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQueryWithHighlightBody(
            String.format("source=%s | chart count() over ts by env", TWOAGG_PATTERN),
            "{\"pre_tags\":[\"@\"],\"post_tags\":[\"@\"],\"fields\":{\"*\":{}}}");

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa");
  }

  /** timechart with a request-body highlight still narrows to one subset and warns. */
  @Test
  public void partialResultNarrowsTimechartWithHighlight() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQueryWithHighlightBody(
            String.format("source=%s | timechart span=1h count() by env", TWOAGG_PATTERN),
            "{\"pre_tags\":[\"@\"],\"post_tags\":[\"@\"],\"fields\":{\"*\":{}}}");

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa");
  }

  @Test
  public void partialResultNarrowsAppendOfTwoAggregationsToOneSubset() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() by env | append [ search source=%s | stats count() by"
                    + " svc ]",
                TWOAGG_PATTERN, TWOAGG_PATTERN));

    // Narrowed to the all-keyword index, so both halves describe the same documents: env is
    // prod/dev, svc is api, and the env-text (qa, cart) and svc-text (stage, web) values are gone.
    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa", "cart", "stage", "web");
    assertTrue("the shared subset must still be counted: " + result, contains(result, "prod"));
  }

  @Test
  public void partialResultNarrowsMultisearchOfTwoAggregationsToOneSubset() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "| multisearch [ search source=%s | stats count() by env ] [ search source=%s |"
                    + " stats count() by svc ]",
                TWOAGG_PATTERN, TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa", "cart", "stage", "web");
  }

  /** appendcol zips the two aggregations; both must be over the same population or a row lies. */
  @Test
  public void partialResultNarrowsAppendcolOfTwoAggregationsToOneSubset() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() by env | appendcol [ stats count() by svc ]",
                TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa", "cart", "stage", "web");
  }

  /** Not a blanket disable: a single aggregation keeps its own, wider subset. */
  @Test
  public void partialResultStillAppliesToASingleAggregation() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env | sort env", TWOAGG_PATTERN));

    // Only env matters here, so only the env-text index is excluded -- stage (svc-text) stays.
    verifyDataRows(result, rows(1, "dev"), rows(2, "prod"), rows(1, "stage"));
    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** The second aggregate reads the first, not the scan: one subset only. */
  @Test
  public void partialResultStillAppliesToChainedStats() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | stats sum(c) as total", TWOAGG_PATTERN));

    verifyDataRows(result, rows(4));
    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** limit=0 drops the top-N pass, leaving one aggregate that can go partial. */
  @Test
  public void partialResultStillAppliesToChartWithoutTopN() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format("source=%s | chart limit=0 count() over ts by env", TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** A join over an aggregating subsearch groups the scan twice; both sides narrow together. */
  @Test
  public void partialResultNarrowsJoinOverAnAggregatingSubsearch() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | join left=l right=r on l.env = r.env"
                    + " [ source=%s | stats count() as c2 by env, svc ]",
                TWOAGG_PATTERN, TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
    assertExcludesConflictedValues(result, "qa", "cart", "stage", "web");
  }

  /** A subquery is still a RexSubQuery at compile time, so the walk has to look inside it. */
  @Test
  public void partialResultNarrowsSubqueryOverAnAggregatingSubsearch() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() by env | where env in [ source=%s | stats count() by env"
                    + " | fields env ]",
                TWOAGG_PATTERN, TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** A chart nested in a subsearch is still a chart. */
  @Test
  public void partialResultNarrowsChartInsideASubsearch() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() by env | append [ search source=%s | chart count() over"
                    + " ts by svc ]",
                TWOAGG_PATTERN, TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** appendpipe re-aggregates the main result; a grouped re-aggregation groups the scan twice. */
  @Test
  public void partialResultNarrowsAppendpipeWithAGroupedAggregation() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | appendpipe [ stats count() by env ]",
                TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  @Test
  public void partialResultStillAppliesToAppendpipeWithAnUngroupedAggregation() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | appendpipe [ stats sum(c) ]",
                TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /**
   * addcoltotals and streamstats hand one aggregate to two branches, making the plan a DAG.
   * Counting per path rather than per node would read that as two aggregations and bar partial
   * mode.
   */
  @Test
  public void partialResultStillAppliesToAddcoltotals() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | addcoltotals c", TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  @Test
  public void partialResultStillAppliesToStreamstats() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | stats count() as c by env | streamstats window=2 sum(c)",
                TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** eventstats and top/rare aggregate through a window, not a second grouping. */
  @Test
  public void partialResultStillAppliesToEventstatsAndTop() throws IOException {
    setPartialResult(true);
    JSONObject eventstats =
        executeQuery(
            String.format(
                "source=%s | eventstats count() as e by env | stats count() by env",
                TWOAGG_PATTERN));
    JSONObject top = executeQuery(String.format("source=%s | top 2 env", TWOAGG_PATTERN));

    assertEquals(1, eventstats.getJSONArray("warnings").length());
    assertEquals(1, top.getJSONArray("warnings").length());
  }

  /**
   * timewrap post-processes a narrowed timechart; the pivot must not swallow the partial-result
   * warning (its finally clears the lifecycle signals, so warnings are drained before it runs).
   */
  @Test
  public void partialResultWarningSurvivesTimewrap() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | timechart span=1h count() by env | timewrap 1d", TWOAGG_PATTERN));

    assertEquals(1, result.getJSONArray("warnings").length());
  }

  /** One aggregation over a union of two patterns: neither branch is narrowed. */
  @Test
  public void partialResultSkippedForOneAggregationOverAUnion() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | append [ search source=%s ] | stats count() by env",
                TWOAGG_PATTERN, PATTERN));

    assertFalse("a union must not be narrowed per branch: " + result, result.has("warnings"));
    assertTrue("both patterns must be counted: " + result, contains(result, "qa"));
  }

  /** No aggregatable subset exists, so there is nothing to narrow to. */
  @Test
  public void partialResultOnAllTextPatternReturnsCompleteResult() throws IOException {
    setPartialResult(true);
    JSONObject result =
        executeQuery(String.format("source=%s | stats count() by env | sort env", ALLTEXT_PATTERN));

    assertFalse("no subset can be kept: " + result, result.has("warnings"));
    verifyDataRows(result, rows(1, "prod"), rows(1, "qa"));
  }

  /** Partial mode is a pushdown path, so it cannot apply when pushdown is off. */
  @Test
  public void partialResultRequiresPushdown() throws IOException {
    setPartialResult(true);
    setPushdown(false);
    try {
      JSONObject result =
          executeQuery(String.format("source=%s | stats count() by env", TWOAGG_PATTERN));

      assertFalse("no pushdown, no partial result: " + result, result.has("warnings"));
      assertTrue("the env-text index must still be counted: " + result, contains(result, "qa"));
    } finally {
      setPushdown(true);
    }
  }

  /**
   * The per-query subset rides a thread-local on pooled workers, so mixed traffic must not
   * cross-contaminate: a chart (union {env, ts}) excludes only the env-text index, a single stats
   * on svc excludes only the svc-text index, and each must see its own subset.
   */
  @Test
  public void perQuerySubsetDoesNotLeakBetweenConcurrentQueries() throws Exception {
    setPartialResult(true);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Callable<String>> jobs = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        boolean chart = i % 2 == 0;
        jobs.add(
            () -> {
              JSONObject result =
                  executeQuery(
                      String.format(
                          chart
                              ? "source=%s | chart count() over ts by env"
                              : "source=%s | stats count() by svc",
                          TWOAGG_PATTERN));
              String rows = result.getJSONArray("datarows").toString();
              // chart (union {env, ts}) excludes only the env-text index: no qa, but stage
              // (svc-text
              // env) stays. svc-stats (union {svc}) excludes only the svc-text index: no web, but
              // cart (env-text svc) stays. A leaked subset would drop the wrong value.
              boolean ok =
                  chart
                      ? !rows.contains("\"qa\"") && rows.contains("\"stage\"")
                      : !rows.contains("\"web\"") && rows.contains("\"cart\"");
              return ok
                  ? "ok"
                  : String.format("%s leaked: %s", chart ? "chart" : "svc-stats", rows);
            });
      }
      List<String> outcomes = new ArrayList<>();
      for (Future<String> future : pool.invokeAll(jobs)) {
        outcomes.add(future.get());
      }
      assertEquals(
          "every query must see its own subset: " + outcomes,
          List.of("ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok"),
          outcomes);
    } finally {
      pool.shutdown();
    }
  }

  private static String[] pair(String env, String svc) {
    return new String[] {env, svc};
  }

  /** A narrowed result must contain none of the values that live only in an excluded index. */
  private void assertExcludesConflictedValues(JSONObject result, String... conflicted) {
    String rows = result.getJSONArray("datarows").toString();
    for (String value : conflicted) {
      assertFalse(
          "excluded value [" + value + "] must not appear in a narrowed result: " + result,
          rows.contains("\"" + value + "\""));
    }
  }

  private boolean contains(JSONObject result, String value) {
    return result.getJSONArray("datarows").toString().contains("\"" + value + "\"");
  }

  private void createBareTextIndex(String index, String env) throws IOException {
    if (isIndexExist(client(), index)) {
      return;
    }
    createIndexByRestClient(
        client(),
        index,
        "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
            + "\"mappings\":{\"properties\":{\"env\":{\"type\":\"text\"}}}}");
    Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
    bulk.setJsonEntity(String.format("{\"index\":{}}\n{\"env\":\"%s\"}\n", env));
    performRequest(client(), bulk);
  }

  private void setPushdown(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
            Boolean.toString(enabled)));
  }

  /** One index of the two-aggregation fixture; each doc is an {@code {env, svc}} pair. */
  private void createTwoAggIndex(String index, String envType, String svcType, String[]... docs)
      throws IOException {
    if (isIndexExist(client(), index)) {
      return;
    }
    createIndexByRestClient(
        client(),
        index,
        String.format(
            "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":0}},"
                + "\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},"
                + "\"ts\":{\"type\":\"date\"},\"env\":{\"type\":\"%s\"},\"svc\":{\"type\":\"%s\"}}}}",
            envType, svcType));
    StringBuilder bulk = new StringBuilder();
    for (String[] doc : docs) {
      bulk.append("{\"index\":{}}\n")
          .append(
              String.format(
                  "{\"@timestamp\":\"2026-01-01T00:00:00Z\",\"ts\":\"2026-01-01T00:00:00Z\","
                      + "\"env\":\"%s\",\"svc\":\"%s\"}\n",
                  doc[0], doc[1]));
    }
    Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
    request.setJsonEntity(bulk.toString());
    performRequest(client(), request);
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
