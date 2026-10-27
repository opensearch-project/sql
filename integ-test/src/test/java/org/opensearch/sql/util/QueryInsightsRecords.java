/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Reads Query Insights Top N records for the ITs that run with Query Insights installed.
 *
 * <p>A PPL record is matched by the index its query text reads, since the record keeps the query as
 * written, so each scenario should read an index of its own.
 */
public final class QueryInsightsRecords {

  public static final String PPL_ENABLED = "search.insights.top_queries.ppl.enabled";

  private QueryInsightsRecords() {}

  /**
   * Fails, naming the cause, when the installed Query Insights predates the PPL consumer: without
   * it the gate setting is unregistered and no record would ever arrive.
   */
  public static void assertSupportsPpl(RestClient client) throws IOException {
    Request request = new Request("GET", "/_cluster/settings");
    request.addParameter("include_defaults", "true");
    request.addParameter("flat_settings", "true");
    JSONObject settings = new JSONObject(getResponseBody(client.performRequest(request)));
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

  /** Records PPL queries, with room for every query a suite runs and the smallest window. */
  public static void enableRecording(RestClient client) throws IOException {
    putPersistentSettings(
        client,
        Map.of(
            PPL_ENABLED,
            "true",
            "search.insights.top_queries.latency.enabled",
            "true",
            // Room for every query a suite runs, so none is evicted from the window it is read
            // from.
            "search.insights.top_queries.latency.top_n_size",
            "100",
            // The smallest window, so records reach the exported store within a minute.
            "search.insights.top_queries.latency.window_size",
            "1m"));
  }

  public static void putPersistentSettings(RestClient client, Map<String, String> settings)
      throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(new JSONObject().put("persistent", new JSONObject(settings)).toString());
    client.performRequest(request);
  }

  /** Creates {@code index} with two documents over a keyword {@code k} and an integer {@code v}. */
  public static void indexWithDocs(RestClient client, String index) throws IOException {
    Request create = new Request("PUT", "/" + index);
    create.setJsonEntity(
        "{\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},"
            + " \"mappings\": {\"properties\": {\"k\": {\"type\": \"keyword\"},"
            + " \"v\": {\"type\": \"integer\"}}}}");
    client.performRequest(create);
    Request bulk = new Request("POST", "/" + index + "/_bulk");
    bulk.addParameter("refresh", "true");
    bulk.setJsonEntity(
        "{\"index\": {}}\n{\"k\": \"a\", \"v\": 1}\n{\"index\": {}}\n{\"k\": \"b\", \"v\": 2}\n");
    client.performRequest(bulk);
  }

  /**
   * Top N overview records since {@code sinceMillis}, read with {@code options} (pass an
   * Authorization header to read as a particular user). The time range makes the API merge the
   * in-memory window with the exported store, so a record is found even after its window has rolled
   * over.
   */
  public static List<JSONObject> topQueries(
      RestClient client, RequestOptions options, long sinceMillis, Predicate<JSONObject> match)
      throws IOException {
    Request request = new Request("GET", "/_insights/top_queries");
    request.addParameter("type", "latency");
    request.addParameter("from", Instant.ofEpochMilli(sinceMillis - 60_000).toString());
    request.addParameter("to", Instant.now().plusSeconds(60).toString());
    request.setOptions(options);
    JSONArray records =
        new JSONObject(getResponseBody(client.performRequest(request))).optJSONArray("top_queries");
    return select(records, match);
  }

  public static JSONObject awaitRecord(
      RestClient client,
      RequestOptions options,
      long sinceMillis,
      Predicate<JSONObject> match,
      String what)
      throws Exception {
    AtomicReference<JSONObject> found = new AtomicReference<>();
    OpenSearchTestCase.assertBusy(
        () -> {
          List<JSONObject> records = topQueries(client, options, sinceMillis, match);
          assertFalse("no " + what + " yet", records.isEmpty());
          found.set(records.get(0));
        },
        60,
        TimeUnit.SECONDS);
    return found.get();
  }

  /**
   * The child records filed under {@code parentMarker}. Children are kept out of the Top N
   * overview, so they are read from the exported store, which is written when the window rolls
   * over.
   */
  public static List<JSONObject> awaitChildren(RestClient client, String parentMarker, int atLeast)
      throws Exception {
    AtomicReference<List<JSONObject>> found = new AtomicReference<>();
    OpenSearchTestCase.assertBusy(
        () -> {
          Request search = new Request("POST", "/top_queries-*/_search");
          search.setJsonEntity(
              new JSONObject()
                  .put("size", 50)
                  .put(
                      "query",
                      new JSONObject()
                          .put("term", new JSONObject().put("derived_from", parentMarker)))
                  .toString());
          JSONArray hits =
              new JSONObject(getResponseBody(client.performRequest(search)))
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

  /** The PPL record of a query that reads {@code index} with {@code source=}. */
  public static Predicate<JSONObject> pplRecordFor(String index) {
    // Bounded on both sides, so qi_it_barrier_1 does not match source=qi_it_barrier_10.
    Pattern source = Pattern.compile("(?<![\\w-])source=" + Pattern.quote(index) + "(?![\\w-])");
    return record -> isPpl(record) && source.matcher(record.optString("source")).find();
  }

  public static boolean isPpl(JSONObject record) {
    return "PPL".equals(record.optString("query_source"));
  }

  public static Set<String> indicesOf(JSONObject record) {
    return stringsOf(record.optJSONArray("indices"));
  }

  public static Set<String> stringsOf(JSONArray array) {
    Set<String> values = new HashSet<>();
    if (array != null) {
      for (int i = 0; i < array.length(); i++) {
        values.add(array.getString(i));
      }
    }
    return values;
  }

  public static long measurement(JSONObject record, String metric) {
    JSONObject value = record.getJSONObject("measurements").optJSONObject(metric);
    return value == null ? 0L : value.getLong("number");
  }

  public static List<JSONObject> select(JSONArray records, Predicate<JSONObject> match) {
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
}
