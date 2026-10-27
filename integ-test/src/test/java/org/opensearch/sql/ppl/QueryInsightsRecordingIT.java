/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.QueryInsightsRecords.PPL_ENABLED;
import static org.opensearch.sql.util.QueryInsightsRecords.awaitChildren;
import static org.opensearch.sql.util.QueryInsightsRecords.indicesOf;
import static org.opensearch.sql.util.QueryInsightsRecords.isPpl;
import static org.opensearch.sql.util.QueryInsightsRecords.measurement;
import static org.opensearch.sql.util.QueryInsightsRecords.pplRecordFor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.QueryInsightsRecords;

/**
 * End to end: PPL queries are recorded into Query Insights Top N on a cluster that has Query
 * Insights installed.
 *
 * <p>Runs only in {@code :integ-test:integTestWithQueryInsights}, whose cluster installs Query
 * Insights; the default integTest cluster does not, and {@link QueryInsightsReportingIT} covers
 * that case.
 *
 * <p>Each scenario reads its own index, so a record is identified by the index its query text
 * reads. Absence is only asserted after a later query's record has arrived, so a missing record
 * means it was never sent rather than not yet processed.
 */
public class QueryInsightsRecordingIT extends PPLIntegTestCase {

  /** Records are stamped with their query's start, so everything this class records is later. */
  private static final long CLASS_START_MILLIS = System.currentTimeMillis();

  private static final AtomicInteger BARRIER_SEQ = new AtomicInteger();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite(); // join runs on the Calcite path
    QueryInsightsRecords.assertSupportsPpl(client());
    QueryInsightsRecords.enableRecording(client());
  }

  @Test
  public void successfulQueryIsRecorded() throws Exception {
    indexWithDocs("qi_it_success");
    executeQuery("source=qi_it_success | where v > 1 | fields v");

    JSONObject record = awaitPplRecord("qi_it_success");
    assertFalse(record.getBoolean("failed"));
    // As written, literal included, the way a DSL search's source is recorded.
    assertEquals("source=qi_it_success | where v > 1 | fields v", record.getString("source"));
    assertTrue(
        "parent marker must be <source>:<nodeId>:<taskId>, was "
            + record.optString("parent_marker"),
        record.optString("parent_marker").matches("PPL:[^:]+:\\d+"));
    assertTrue(record.getLong("timestamp") >= CLASS_START_MILLIS);
    // The plugin measures its own threads; zero here means the accounting never ran.
    assertTrue(measurement(record, "cpu") > 0);
    assertTrue(measurement(record, "memory") > 0);
  }

  @Test
  public void queryOnMissingIndexIsRecordedAsFailed() throws Exception {
    expectThrows(ResponseException.class, () -> executeQuery("source=qi_it_missing | fields v"));

    // Fails in analysis, before any search runs; the query parsed, so it is still recorded.
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
    // A query that never parses is not reported.
    assertNoPplRecord("qi_it_syntax");
  }

  @Test
  public void nothingIsRecordedWhenTheGateIsOff() throws Exception {
    indexWithDocs("qi_it_gated");
    QueryInsightsRecords.putPersistentSettings(client(), Map.of(PPL_ENABLED, "false"));
    try {
      executeQuery("source=qi_it_gated | where v > 1 | fields v");
    } finally {
      QueryInsightsRecords.putPersistentSettings(client(), Map.of(PPL_ENABLED, "true"));
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

    List<JSONObject> children = awaitChildren(client(), parent.getString("parent_marker"), 2);
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
    // The record names no indices; its searches carry the join's two sides.
    assertEquals(Set.of("qi_it_join_left", "qi_it_join_right"), childIndices);
  }

  // ---------------------------------------------------------------------------------------------

  private static void indexWithDocs(String index) throws Exception {
    QueryInsightsRecords.indexWithDocs(client(), index);
  }

  /** Runs a recorded query and waits for its record, so everything sent before it has arrived. */
  private void awaitBarrier() throws Exception {
    String index = "qi_it_barrier_" + BARRIER_SEQ.incrementAndGet();
    indexWithDocs(index);
    executeQuery("source=" + index + " | fields v");
    awaitPplRecord(index);
  }

  private static JSONObject awaitPplRecord(String index) throws Exception {
    return awaitRecord(pplRecordFor(index), "PPL record for " + index);
  }

  private static void assertNoPplRecord(String index) throws Exception {
    List<JSONObject> found = topQueries(pplRecordFor(index));
    assertTrue("unexpected PPL record for " + index + ": " + found, found.isEmpty());
  }

  private static JSONObject awaitRecord(Predicate<JSONObject> match, String what) throws Exception {
    return QueryInsightsRecords.awaitRecord(
        client(), RequestOptions.DEFAULT, CLASS_START_MILLIS, match, what);
  }

  private static List<JSONObject> topQueries(Predicate<JSONObject> match) throws Exception {
    return QueryInsightsRecords.topQueries(
        client(), RequestOptions.DEFAULT, CLASS_START_MILLIS, match);
  }
}
