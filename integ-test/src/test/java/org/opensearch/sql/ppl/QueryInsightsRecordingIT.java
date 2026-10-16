/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * End to end: PPL queries are recorded into Query Insights Top N on a cluster that has Query
 * Insights installed.
 *
 * <p>Runs only in {@code :integ-test:integTestWithQueryInsights}, whose cluster installs Query
 * Insights; the default integTest cluster does not, and {@link QueryInsightsReportingIT} covers
 * that case.
 *
 * <p>Each scenario reads its own index, so a record is identified by the index it reports rather
 * than by its query text, which is anonymized. Absence is only asserted after a later query's
 * record has arrived, so a missing record means it was never sent rather than not yet processed.
 */
public class QueryInsightsRecordingIT extends PPLIntegTestCase {

  private static final String PPL_ENABLED = "search.insights.top_queries.ppl.enabled";

  /** Records are stamped with their query's start, so everything this class records is later. */
  private static final long CLASS_START_MILLIS = System.currentTimeMillis();

  private static final AtomicInteger BARRIER_SEQ = new AtomicInteger();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite(); // join runs on the Calcite path
    assertQueryInsightsSupportsPpl();
    updateClusterSettings(new ClusterSetting(PERSISTENT, PPL_ENABLED, "true"));
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, "search.insights.top_queries.latency.enabled", "true"));
    // Room for every query this class runs, so none is evicted from the window it is read from.
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, "search.insights.top_queries.latency.top_n_size", "100"));
    // The smallest window, so records reach the exported store within a minute.
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, "search.insights.top_queries.latency.window_size", "1m"));
  }

  @Test
  public void successfulQueryIsRecorded() throws Exception {
    indexWithDocs("qi_it_success");
    executeQuery("source=qi_it_success | where v > 1 | fields v");

    JSONObject record = awaitPplRecord("qi_it_success");
    assertFalse(record.getBoolean("failed"));
    assertEquals(Set.of("qi_it_success"), indicesOf(record));
    assertTrue(
        "parent marker must be <source>:<nodeId>:<taskId>, was "
            + record.optString("parent_marker"),
        record.optString("parent_marker").matches("PPL:[^:]+:\\d+"));
    assertFalse("query text must be recorded", record.optString("source").isEmpty());
    assertTrue(record.getLong("timestamp") >= CLASS_START_MILLIS);
    // The plugin measures its own threads; zero here means the accounting never ran.
    assertTrue(measurement(record, "cpu") > 0);
    assertTrue(measurement(record, "memory") > 0);
  }

  @Test
  public void queryOnMissingIndexIsRecordedAsFailed() throws Exception {
    expectThrows(ResponseException.class, () -> executeQuery("source=qi_it_missing | fields v"));

    // Fails in analysis, before any search runs; the record still names the index it targeted.
    JSONObject record = awaitPplRecord("qi_it_missing");
    assertTrue(record.getBoolean("failed"));
  }

  @Test
  public void queryOnUnknownFieldIsRecordedAsFailed() throws Exception {
    indexWithDocs("qi_it_bad_field");
    expectThrows(
        ResponseException.class,
        () -> executeQuery("source=qi_it_bad_field | where no_such_field > 1 | fields v"));

    JSONObject record = awaitPplRecord("qi_it_bad_field");
    assertTrue(record.getBoolean("failed"));
  }

  @Test
  public void explainIsNotRecorded() throws Exception {
    indexWithDocs("qi_it_explain");
    explainQueryToString("source=qi_it_explain | fields v");
    // explain is also valid inside the query text, which reaches the execute route.
    executeQuery("explain source=qi_it_explain | fields v");

    awaitBarrier();
    assertNoPplRecord("qi_it_explain");
  }

  @Test
  public void syntaxErrorIsNotRecorded() throws Exception {
    expectThrows(ResponseException.class, () -> executeQuery("source=qi_it_syntax | wherr v > 1"));

    awaitBarrier();
    // A query that never parses has no text or indices to report. Every other query in this class
    // names an index, so a PPL record without one could only be the syntax error.
    for (JSONObject record : topQueries(QueryInsightsRecordingIT::isPpl)) {
      assertFalse("unparsed query was recorded: " + record, indicesOf(record).isEmpty());
    }
  }

  @Test
  public void nothingIsRecordedWhenTheGateIsOff() throws Exception {
    indexWithDocs("qi_it_gated");
    updateClusterSettings(new ClusterSetting(PERSISTENT, PPL_ENABLED, "false"));
    try {
      executeQuery("source=qi_it_gated | where v > 1 | fields v");
    } finally {
      updateClusterSettings(new ClusterSetting(PERSISTENT, PPL_ENABLED, "true"));
    }

    awaitBarrier();
    assertNoPplRecord("qi_it_gated");
    // The search it ran is still recorded as an ordinary query: with the gate off no parent marker
    // is stamped, so it is nobody's child and stays in the Top N overview.
    JSONObject search =
        awaitRecord(r -> !isPpl(r) && indicesOf(r).contains("qi_it_gated"), "native search");
    assertFalse(search.optBoolean("is_child", false));
  }

  @Test
  public void asyncSubmissionIsNotRecorded() throws Exception {
    indexWithDocs("qi_it_async");
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        "{\"query\": \"source=qi_it_async | fields v\", \"wait_for_completion_timeout\": \"30s\"}");
    client().performRequest(request);

    // The query outlives its task on this route, so a record built when the task ends would carry
    // near-zero latency; the route is left out of Top N.
    awaitBarrier();
    assertNoPplRecord("qi_it_async");
  }

  @Test
  public void childSearchesCarryTheParentMarker() throws Exception {
    indexWithDocs("qi_it_join_left");
    indexWithDocs("qi_it_join_right");
    executeQuery(
        "source=qi_it_join_left | join left=l right=r on l.k = r.k qi_it_join_right"
            + " | fields l.v, r.v");

    JSONObject parent = awaitPplRecord("qi_it_join_left");
    assertEquals(Set.of("qi_it_join_left", "qi_it_join_right"), indicesOf(parent));

    // Children are kept out of the Top N overview, so read them from the exported store.
    List<JSONObject> children = awaitChildren(parent.getString("parent_marker"), 2);
    Set<String> childIndices = new HashSet<>();
    for (JSONObject child : children) {
      assertTrue(child.getBoolean("is_child"));
      assertEquals("DSL", child.getString("query_source"));
      // Stamped with the query's start, the record never sorts after the searches it spawned; a
      // completion-time stamp could put it in a later Top N window than its children.
      assertTrue(
          "parent " + parent.getLong("timestamp") + " after child " + child.getLong("timestamp"),
          parent.getLong("timestamp") <= child.getLong("timestamp"));
      childIndices.addAll(indicesOf(child));
    }
    // The searches read exactly the indices the record names.
    assertEquals(indicesOf(parent), childIndices);
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * Fails fast, naming the cause, when the installed Query Insights predates the PPL consumer:
   * without it the gate setting is unregistered and no record would ever arrive.
   */
  private static void assertQueryInsightsSupportsPpl() throws IOException {
    Request request = new Request("GET", "/_cluster/settings");
    request.addParameter("include_defaults", "true");
    request.addParameter("flat_settings", "true");
    JSONObject settings = new JSONObject(getResponseBody(client().performRequest(request)));
    // Listed under defaults until a test sets it, then under persistent.
    boolean registered = false;
    for (String section : List.of("defaults", "persistent", "transient")) {
      JSONObject values = settings.optJSONObject(section);
      registered |= values != null && values.has(PPL_ENABLED);
    }
    assertTrue(
        "Query Insights in this cluster does not register "
            + PPL_ENABLED
            + ", so it predates the PPL consumer. Run with -PqueryInsightsZip=<path to a Query"
            + " Insights build that includes it>.",
        registered);
  }

  private static void indexWithDocs(String index) throws IOException {
    Request create = new Request("PUT", "/" + index);
    create.setJsonEntity(
        "{\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + " \"mappings\": {\"properties\": {\"k\": {\"type\": \"keyword\"},"
            + " \"v\": {\"type\": \"integer\"}}}}");
    client().performRequest(create);
    Request bulk = new Request("POST", "/" + index + "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        "{\"index\": {}}\n{\"k\": \"a\", \"v\": 1}\n{\"index\": {}}\n{\"k\": \"b\", \"v\": 2}\n");
    client().performRequest(bulk);
  }

  /** Runs a recorded query and waits for its record, so everything sent before it has arrived. */
  private void awaitBarrier() throws Exception {
    String index = "qi_it_barrier_" + BARRIER_SEQ.incrementAndGet();
    indexWithDocs(index);
    executeQuery("source=" + index + " | fields v");
    awaitPplRecord(index);
  }

  private static JSONObject awaitPplRecord(String index) throws Exception {
    return awaitRecord(r -> isPpl(r) && indicesOf(r).contains(index), "PPL record for " + index);
  }

  private static void assertNoPplRecord(String index) throws IOException {
    List<JSONObject> found = topQueries(r -> isPpl(r) && indicesOf(r).contains(index));
    assertTrue("unexpected PPL record for " + index + ": " + found, found.isEmpty());
  }

  private static JSONObject awaitRecord(Predicate<JSONObject> match, String what) throws Exception {
    AtomicReference<JSONObject> found = new AtomicReference<>();
    assertBusy(
        () -> {
          List<JSONObject> records = topQueries(match);
          assertFalse("no " + what + " yet", records.isEmpty());
          found.set(records.get(0));
        },
        60,
        TimeUnit.SECONDS);
    return found.get();
  }

  /**
   * Top N overview over this class's run. The time range makes the API merge the in-memory window
   * with the exported store, so a record is found even after its window has rolled over.
   */
  private static List<JSONObject> topQueries(Predicate<JSONObject> match) throws IOException {
    Request request = new Request("GET", "/_insights/top_queries");
    request.addParameter("type", "latency");
    request.addParameter("from", Instant.ofEpochMilli(CLASS_START_MILLIS - 60_000).toString());
    request.addParameter("to", Instant.now().plusSeconds(60).toString());
    JSONArray records =
        new JSONObject(getResponseBody(client().performRequest(request)))
            .optJSONArray("top_queries");
    return select(records, match);
  }

  private static List<JSONObject> awaitChildren(String parentMarker, int atLeast) throws Exception {
    AtomicReference<List<JSONObject>> found = new AtomicReference<>();
    // The store is written when the window rolls over, so allow a few windows.
    assertBusy(
        () -> {
          Request search = new Request("POST", "/top_queries-*/_search");
          search.setJsonEntity(
              "{\"size\": 50, \"query\": {\"term\": {\"derived_from\": \""
                  + parentMarker
                  + "\"}}}");
          JSONArray hits =
              new JSONObject(getResponseBody(client().performRequest(search)))
                  .getJSONObject("hits")
                  .getJSONArray("hits");
          List<JSONObject> children = new ArrayList<>();
          for (int i = 0; i < hits.length(); i++) {
            children.add(hits.getJSONObject(i).getJSONObject("_source"));
          }
          assertTrue(
              "children of " + parentMarker + " so far: " + children.size(),
              children.size() >= atLeast);
          found.set(children);
        },
        3,
        TimeUnit.MINUTES);
    return found.get();
  }

  private static List<JSONObject> select(JSONArray records, Predicate<JSONObject> match) {
    List<JSONObject> selected = new ArrayList<>();
    if (records != null) {
      for (int i = 0; i < records.length(); i++) {
        JSONObject record = records.getJSONObject(i);
        if (match.test(record)) {
          selected.add(record);
        }
      }
    }
    return selected;
  }

  private static boolean isPpl(JSONObject record) {
    return "PPL".equals(record.optString("query_source"));
  }

  private static Set<String> indicesOf(JSONObject record) {
    Set<String> indices = new HashSet<>();
    JSONArray array = record.optJSONArray("indices");
    if (array != null) {
      for (int i = 0; i < array.length(); i++) {
        indices.add(array.getString(i));
      }
    }
    return indices;
  }

  private static long measurement(JSONObject record, String metric) {
    JSONObject value = record.getJSONObject("measurements").optJSONObject(metric);
    return value == null ? 0L : value.getLong("number");
  }
}
