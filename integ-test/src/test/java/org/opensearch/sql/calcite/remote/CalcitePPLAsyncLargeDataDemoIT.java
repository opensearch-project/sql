/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Large-data correctness and latency report for PPL asynchronous partial results.
 *
 * <p>The dedicated Gradle task provisions three OpenSearch nodes with a 16 GiB heap per node and a
 * 16 GiB test JVM. Full REST responses are written under {@code
 * integ-test/build/reports/ppl-async-large-data-demo}.
 */
public class CalcitePPLAsyncLargeDataDemoIT extends PPLIntegTestCase {
  private static final String INDEX = "logs-00001";
  private static final String INDEX_TEMPLATE = "log-template";
  private static final int ROW_COUNT = Integer.getInteger("ppl.demo.rows", 4_000_000);
  private static final int SHARD_COUNT = 12;
  private static final int SEARCH_PAGE_SIZE = 10_000;
  private static final int BULK_BATCH_SIZE = 10_000;
  private static final int PRODUCT_COUNT = 10_000;
  private static final int CLUSTER_COUNT = 1_000;
  private static final long TIME_BUCKET_MILLIS = 30_000L;
  private static final long TIMESTAMP_STEP_MILLIS = 100L;
  private static final long BASE_TIMESTAMP_MILLIS =
      Instant.parse("2025-09-04T16:00:00Z").toEpochMilli();
  private static final int APPEND_RESULT_COUNT = Math.min(250_000, ROW_COUNT);
  private static final int RESPONSE_COUNT = 10_000;
  private static final long MIN_HEAP_BYTES = 16L * 1024 * 1024 * 1024;
  private static final Path REPORT_DIRECTORY =
      Path.of(
          System.getProperty("project.root", "."), "build", "reports", "ppl-async-large-data-demo");

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQueryBucketSize(500);
  }

  @After
  public void tearDown() throws Exception {
    resetQueryBucketSize();
    super.tearDown();
  }

  @Test
  public void testSixOtelQueryPatternsMatchSynchronousResults() throws Exception {
    prepareReportDirectory();
    assertClusterHeap();
    createOtelLogIndex();

    List<QueryPattern> patterns =
        List.of(
            new QueryPattern(
                "rex_append",
                String.format(
                    Locale.ROOT,
                    "source=%s"
                        + " | rex field=body \"level[^a-z]+(?<loglevel>error|warn|info)\""
                        + " | fields `@timestamp`, severityText, body, loglevel"
                        + " | head %d",
                    INDEX,
                    APPEND_RESULT_COUNT),
                "APPEND",
                true,
                false,
                APPEND_RESULT_COUNT),
            new QueryPattern(
                "fully_pushed_aggregation",
                String.format(
                    Locale.ROOT,
                    "source=%s"
                        + " | stats sum(`attributes.obs_body_length`) as total_body_bytes,"
                        + " avg(severityNumber) as avg_severity,"
                        + " max(flags) as max_flags,"
                        + " min(severityNumber) as min_severity",
                    INDEX),
                "REPLACE",
                true,
                false,
                1),
            new QueryPattern(
                "composite_post_processing",
                String.format(
                    Locale.ROOT,
                    "source=%s"
                        + " | stats count() as total by `resource.attributes.productid`"
                        + " | eval doubled = total * 2"
                        + " | fields `resource.attributes.productid`, total, doubled",
                    INDEX),
                "REPLACE",
                true,
                false,
                Math.min(PRODUCT_COUNT, ROW_COUNT)),
            new QueryPattern(
                "eventstats_blocking",
                String.format(
                    Locale.ROOT,
                    "source=%s"
                        + " | rex field=body"
                        + " \"caller[^a-z]+(?<caller>[a-z]+/[a-z]+[.]go)\""
                        + " | eventstats count() as product_log_count"
                        + " by `resource.attributes.productid`"
                        + " | where severityText = 'ERROR'"
                        + " | fields `@timestamp`, severityText,"
                        + " `resource.attributes.productid`,"
                        + " `attributes.cluster.name`, caller, product_log_count",
                    INDEX),
                "REPLACE",
                false,
                false,
                divideRoundingUp(ROW_COUNT, 1_000)),
            new QueryPattern(
                "sort_blocking",
                String.format(
                    Locale.ROOT,
                    "source=%s"
                        + " | where `attributes.cluster.name` ="
                        + " 'xyz-cluster0-ci-prod-us-east-1'"
                        + " | sort - `@timestamp`"
                        + " | fields `@timestamp`, severityText, body,"
                        + " `attributes.cluster.name`",
                    INDEX),
                "REPLACE",
                false,
                true,
                divideRoundingUp(ROW_COUNT, CLUSTER_COUNT)),
            new QueryPattern(
                "timespan_aggregation",
                String.format(
                    Locale.ROOT, "source=%s | stats count() by span(@timestamp, 30s)", INDEX),
                "REPLACE",
                true,
                false,
                timeBucketCount()));

    List<PatternResult> results = new ArrayList<>();
    for (QueryPattern pattern : patterns) {
      results.add(runPattern(pattern));
    }
    writeSummary(results);
  }

  private PatternResult runPattern(QueryPattern pattern) throws Exception {
    long synchronousStart = System.nanoTime();
    JSONObject synchronous = executeSynchronously(pattern.query());
    long synchronousMillis = elapsedMillis(synchronousStart);
    writeResponse(pattern.name(), "synchronous", synchronous);

    Assert.assertEquals(
        pattern.name() + " synchronous row count",
        pattern.expectedRows(),
        synchronous.getJSONArray("datarows").length());

    long asyncStart = System.nanoTime();
    JSONObject snapshot = submit(pattern.query());
    long submitMillis = elapsedMillis(asyncStart);
    String id = snapshot.getString("id");
    JSONObject firstProgress = null;
    long firstProgressMillis = -1;
    JSONObject firstUseful = null;
    long firstUsefulMillis = -1;

    for (int attempt = 0;
        attempt < 20_000 && !"SUCCEEDED".equals(snapshot.getString("status"));
        attempt++) {
      snapshot = poll(id);
      if (snapshot.has("update_mode")) {
        Assert.assertEquals(
            pattern.name() + " update mode",
            pattern.updateMode(),
            snapshot.getString("update_mode"));
      }

      if ("RUNNING".equals(snapshot.getString("status"))) {
        JSONObject progress = snapshot.optJSONObject("progress");
        if (firstProgress == null && progress != null && progress.getDouble("fraction_done") > 0D) {
          firstProgress = snapshot;
          firstProgressMillis = elapsedMillis(asyncStart);
        }
        if (pattern.hasRunningRows() && snapshot.getJSONArray("datarows").length() > 0) {
          if (firstUseful == null) {
            firstUseful = snapshot;
            firstUsefulMillis = elapsedMillis(asyncStart);
          }
        } else if (!pattern.hasRunningRows()) {
          Assert.assertEquals(
              pattern.name() + " must not expose running rows",
              0,
              snapshot.getJSONArray("datarows").length());
          Assert.assertEquals(pattern.name() + " running total", 0, snapshot.getInt("total"));
          if (firstUseful == null && progress != null && progress.getDouble("fraction_done") > 0D) {
            firstUseful = snapshot;
            firstUsefulMillis = elapsedMillis(asyncStart);
          }
        }
      }
      Thread.sleep(50L);
    }

    long finalMillis = elapsedMillis(asyncStart);
    JSONObject finalResponse = snapshot;
    Assert.assertEquals("SUCCEEDED", finalResponse.getString("status"));
    Assert.assertEquals(pattern.updateMode(), finalResponse.getString("update_mode"));
    Assert.assertEquals(pattern.expectedRows(), finalResponse.getInt("total"));
    Assert.assertNotNull(pattern.name() + " first progress response", firstProgress);
    Assert.assertNotNull(pattern.name() + " first useful response", firstUseful);
    Assert.assertTrue(
        pattern.name() + " first useful response must precede final",
        firstUsefulMillis < finalMillis);

    FinalPages finalPages = fetchFinalPages(pattern, id, finalResponse);
    assertSameFinalResult(pattern, synchronous, finalResponse, finalPages.rows());
    if (pattern.hasRunningRows() && "APPEND".equals(pattern.updateMode())) {
      assertStablePrefix(pattern, firstUseful, finalPages.rows());
    }

    writeResponse(pattern.name(), "progress", firstProgress);
    writeResponse(pattern.name(), "first", firstUseful);
    writeResponse(pattern.name(), "final", finalResponse);
    writeFinalPages(pattern.name(), finalPages.responses());
    report(
        pattern,
        submitMillis,
        firstProgressMillis,
        firstUsefulMillis,
        finalMillis,
        synchronousMillis,
        firstUseful);
    return new PatternResult(
        pattern,
        submitMillis,
        firstProgressMillis,
        firstUsefulMillis,
        finalMillis,
        synchronousMillis,
        firstUseful.getJSONArray("datarows").length(),
        finalResponse.getInt("total"),
        finalPages.responses().size());
  }

  private static void assertSameFinalResult(
      QueryPattern pattern, JSONObject synchronous, JSONObject finalResponse, JSONArray finalRows) {
    Assert.assertEquals(
        pattern.name() + " schema",
        synchronous.getJSONArray("schema").toString(),
        finalResponse.getJSONArray("schema").toString());
    if (pattern.ordered()) {
      Assert.assertEquals(
          pattern.name() + " ordered final rows",
          synchronous.getJSONArray("datarows").toString(),
          finalRows.toString());
      return;
    }
    List<String> synchronousRows = canonicalRows(synchronous.getJSONArray("datarows"));
    List<String> asyncRows = canonicalRows(finalRows);
    Assert.assertEquals(pattern.name() + " final rows", synchronousRows, asyncRows);
  }

  private static void assertStablePrefix(
      QueryPattern pattern, JSONObject firstUseful, JSONArray finalRows) {
    JSONArray partialRows = firstUseful.getJSONArray("datarows");
    Assert.assertTrue(partialRows.length() > 0);
    Assert.assertTrue(partialRows.length() <= finalRows.length());
    for (int row = 0; row < partialRows.length(); row++) {
      Assert.assertEquals(
          pattern.name() + " stable-prefix row " + row,
          partialRows.getJSONArray(row).toString(),
          finalRows.getJSONArray(row).toString());
    }
  }

  private FinalPages fetchFinalPages(QueryPattern pattern, String id, JSONObject firstFinalResponse)
      throws Exception {
    int total = firstFinalResponse.getInt("total");
    List<JSONObject> responses = new ArrayList<>();
    JSONArray rows = new JSONArray();
    for (int offset = 0; offset < total; offset += RESPONSE_COUNT) {
      JSONObject page = offset == 0 ? firstFinalResponse : getPage(id, offset);
      Assert.assertEquals(
          pattern.name() + " final page status", "SUCCEEDED", page.getString("status"));
      Assert.assertEquals(pattern.name() + " final page total", total, page.getInt("total"));
      Assert.assertEquals(
          pattern.name() + " final page update mode",
          pattern.updateMode(),
          page.getString("update_mode"));
      Assert.assertEquals(
          pattern.name() + " final page schema",
          firstFinalResponse.getJSONArray("schema").toString(),
          page.getJSONArray("schema").toString());
      Assert.assertEquals(
          pattern.name() + " final page offset",
          offset,
          page.getJSONObject("window").getInt("offset"));
      responses.add(page);
      JSONArray pageRows = page.getJSONArray("datarows");
      for (int row = 0; row < pageRows.length(); row++) {
        rows.put(pageRows.getJSONArray(row));
      }
    }
    Assert.assertEquals(pattern.name() + " assembled final row count", total, rows.length());
    return new FinalPages(rows, responses);
  }

  private static List<String> canonicalRows(JSONArray rows) {
    List<String> canonical = new ArrayList<>(rows.length());
    for (int row = 0; row < rows.length(); row++) {
      canonical.add(rows.getJSONArray(row).toString());
    }
    canonical.sort(Comparator.naturalOrder());
    return canonical;
  }

  private JSONObject executeSynchronously(String query) throws Exception {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(new JSONObject().put("query", query).toString());
    return perform(request);
  }

  private JSONObject submit(String query) throws Exception {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("wait_for_completion_timeout", "0ms")
            .put("keep_alive", "10m")
            .toString());
    return perform(request);
  }

  private JSONObject poll(String id) throws Exception {
    return perform(
        new Request(
            "GET",
            String.format(
                Locale.ROOT, "/_plugins/_ppl/jobs/%s?offset=0&count=%d", id, RESPONSE_COUNT)));
  }

  private JSONObject getPage(String id, int offset) throws Exception {
    return perform(
        new Request(
            "GET",
            String.format(
                Locale.ROOT,
                "/_plugins/_ppl/jobs/%s?offset=%d&count=%d",
                id,
                offset,
                RESPONSE_COUNT)));
  }

  private void assertClusterHeap() throws Exception {
    JSONObject nodes = perform(new Request("GET", "/_nodes/jvm"));
    Assert.assertEquals(3, nodes.getJSONObject("nodes").length());
    nodes
        .getJSONObject("nodes")
        .toMap()
        .forEach(
            (nodeId, nodeValue) -> {
              JSONObject node = new JSONObject((java.util.Map<?, ?>) nodeValue);
              long heapBytes =
                  node.getJSONObject("jvm").getJSONObject("mem").getLong("heap_max_in_bytes");
              Assert.assertTrue(
                  "Node " + nodeId + " heap is below 16 GiB: " + heapBytes,
                  heapBytes >= MIN_HEAP_BYTES);
              System.out.printf(
                  Locale.ROOT, "PPL_LARGE_DEMO_JVM node=%s heap_max_bytes=%d%n", nodeId, heapBytes);
            });
  }

  private void createOtelLogIndex() throws Exception {
    Request delete = new Request("DELETE", "/" + INDEX);
    delete.addParameter("ignore_unavailable", "true");
    client().performRequest(delete);

    Request template = new Request("PUT", "/_index_template/" + INDEX_TEMPLATE);
    template.setJsonEntity(
        """
        {
          "index_patterns": ["logs-*"],
          "priority": 100,
          "template": {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 1
            },
            "mappings": {
              "dynamic_templates": [
                {
                  "resource_attributes": {
                    "path_match": "resource.attributes.*",
                    "mapping": {"type": "keyword"},
                    "match_mapping_type": "string"
                  }
                },
                {
                  "attributes": {
                    "path_match": "attributes.*",
                    "mapping": {"type": "keyword"},
                    "match_mapping_type": "string"
                  }
                }
              ],
              "properties": {
                "traceId": {"type": "keyword"},
                "flags": {"type": "byte"},
                "severityNumber": {"type": "integer"},
                "body": {"norms": false, "type": "text"},
                "serviceName": {"type": "keyword"},
                "schemaUrl": {"type": "keyword"},
                "spanId": {"type": "keyword"},
                "@timestamp": {"type": "date"},
                "severityText": {"type": "keyword"},
                "@version": {"type": "keyword"},
                "attributes": {
                  "type": "object",
                  "properties": {
                    "time": {"enabled": false, "type": "object"}
                  }
                },
                "time": {"type": "date"},
                "observedTimestamp": {"type": "date"},
                "log": {"type": "keyword"}
              }
            }
          }
        }
        """);
    Assert.assertEquals(200, client().performRequest(template).getStatusLine().getStatusCode());

    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "settings": {
                "number_of_shards": %d,
                "number_of_replicas": 0,
                "refresh_interval": "-1",
                "index.max_result_window": %d
              }
            }
            """,
            SHARD_COUNT,
            SEARCH_PAGE_SIZE));
    Assert.assertEquals(200, client().performRequest(create).getStatusLine().getStatusCode());

    long startNanos = System.nanoTime();
    for (int start = 0; start < ROW_COUNT; start += BULK_BATCH_SIZE) {
      int end = Math.min(ROW_COUNT, start + BULK_BATCH_SIZE);
      StringBuilder body = new StringBuilder((end - start) * 1_024);
      for (int documentId = start; documentId < end; documentId++) {
        appendOtelDocument(body, documentId);
      }
      Request bulk = new Request("POST", "/" + INDEX + "/_bulk");
      bulk.setJsonEntity(body.toString());
      JSONObject response = new JSONObject(getResponseBody(client().performRequest(bulk), true));
      Assert.assertFalse(response.toString(), response.getBoolean("errors"));
    }
    client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    System.out.printf(
        Locale.ROOT,
        "PPL_LARGE_DEMO_INDEX rows=%d shards=%d build_ms=%d%n",
        ROW_COUNT,
        SHARD_COUNT,
        elapsedMillis(startNanos));
  }

  private static void appendOtelDocument(StringBuilder bulkBody, int documentId) {
    int productId = documentId % PRODUCT_COUNT;
    int clusterId = documentId % CLUSTER_COUNT;
    int severityNumber = documentId % 24;
    String severityText =
        documentId % 1_000 == 0 ? "ERROR" : documentId % 100 == 0 ? "WARN" : "INFO";
    String logLevel = severityText.toLowerCase(Locale.ROOT);
    String timestamp =
        Instant.ofEpochMilli(BASE_TIMESTAMP_MILLIS + documentId * TIMESTAMP_STEP_MILLIS).toString();

    bulkBody
        .append("{\"index\":{}}\n")
        .append("{\"traceId\":\"\",")
        .append("\"instrumentationScope\":{\"droppedAttributesCount\":0},")
        .append("\"resource\":{\"droppedAttributesCount\":0,\"attributes\":{")
        .append("\"log_type\":\"EKS_node\",")
        .append("\"k8s_label.productid\":\"pr")
        .append(123_456 + productId)
        .append("\",")
        .append("\"k8s_label.sourcetype\":\"unknown\",")
        .append("\"productid\":\"pr")
        .append(123_456 + productId)
        .append("\",")
        .append("\"k8s.platform\":\"EKS\",")
        .append("\"k8s_label.criticality_code\":\"99\",")
        .append("\"k8s.cluster.business.unit\":\"bu\",")
        .append("\"criticality_code\":\"5\",")
        .append("\"sourcetype\":\"unknown\",")
        .append("\"log_tier\":\"standard\",")
        .append("\"applicationid\":\"ap")
        .append(123_456 + productId)
        .append("\",")
        .append("\"obs_namespace\":\"defaultv1\"},\"schemaUrl\":\"\"},")
        .append("\"flags\":")
        .append(documentId % 4)
        .append(',')
        .append("\"severityNumber\":")
        .append(severityNumber)
        .append(',')
        .append("\"schemaUrl\":\"\",\"spanId\":\"\",")
        .append("\"severityText\":\"")
        .append(severityText)
        .append("\",")
        .append("\"attributes\":{")
        .append("\"cluster.name\":\"xyz-cluster")
        .append(clusterId)
        .append("-ci-prod-us-east-1\",")
        .append("\"cluster.region\":\"us-east-1\",")
        .append("\"log.file.path\":\"/var/log/xyz/abc.log\",")
        .append("\"cluster.env\":\"prod\",")
        .append("\"obs_body_length\":")
        .append(146 + documentId % 64)
        .append("},")
        .append("\"time\":\"")
        .append(timestamp)
        .append("\",\"droppedAttributesCount\":0,")
        .append("\"observedTimestamp\":\"")
        .append(timestamp)
        .append("\",\"@timestamp\":\"")
        .append(timestamp)
        .append("\",\"body\":\"{\\\"msg\\\":\\\"Error finding unassigned IPs for ENI eni-")
        .append(documentId)
        .append("\\\",\\\"caller\\\":\\\"network/eni.go:702\\\",\\\"level\\\":\\\"")
        .append(logLevel)
        .append("\\\",\\\"ts\\\":\\\"")
        .append(timestamp)
        .append("\\\"}\",\"log\":null}\n");
  }

  private static int divideRoundingUp(int dividend, int divisor) {
    return (dividend + divisor - 1) / divisor;
  }

  private static int timeBucketCount() {
    if (ROW_COUNT == 0) {
      return 0;
    }
    return (int) (((ROW_COUNT - 1L) * TIMESTAMP_STEP_MILLIS) / TIME_BUCKET_MILLIS + 1);
  }

  private static void prepareReportDirectory() throws Exception {
    Files.createDirectories(REPORT_DIRECTORY);
    try (Stream<Path> files = Files.list(REPORT_DIRECTORY)) {
      files.filter(Files::isRegularFile).forEach(CalcitePPLAsyncLargeDataDemoIT::deleteReportFile);
    }
  }

  private static void deleteReportFile(Path path) {
    try {
      Files.delete(path);
    } catch (Exception exception) {
      throw new IllegalStateException("Unable to delete stale demo report " + path, exception);
    }
  }

  private JSONObject perform(Request request) throws Exception {
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(getResponseBody(response, true));
  }

  private static void writeResponse(String pattern, String stage, JSONObject response)
      throws Exception {
    Files.writeString(
        REPORT_DIRECTORY.resolve(pattern + "-" + stage + ".json"),
        response.toString(2) + System.lineSeparator());
  }

  private static void writeFinalPages(String pattern, List<JSONObject> responses) throws Exception {
    for (int page = 0; page < responses.size(); page++) {
      writeResponse(
          pattern, String.format(Locale.ROOT, "final-page-%02d", page + 1), responses.get(page));
    }
  }

  private static void writeSummary(List<PatternResult> results) throws Exception {
    JSONArray patterns = new JSONArray();
    for (PatternResult result : results) {
      patterns.put(
          new JSONObject()
              .put("pattern", result.pattern().name())
              .put("query", result.pattern().query())
              .put("update_mode", result.pattern().updateMode())
              .put("synchronous_ms", result.synchronousMillis())
              .put("submit_ms", result.submitMillis())
              .put("first_progress_ms", result.firstProgressMillis())
              .put("first_useful_ms", result.firstUsefulMillis())
              .put("final_ms", result.finalMillis())
              .put(
                  "progress_to_first_rows_ms",
                  result.pattern().hasRunningRows()
                      ? result.firstUsefulMillis() - result.firstProgressMillis()
                      : JSONObject.NULL)
              .put("first_to_final_lead_ms", result.finalMillis() - result.firstUsefulMillis())
              .put("first_rows", result.firstRows())
              .put("final_rows", result.finalRows())
              .put("final_page_count", result.finalPageCount())
              .put("final_equals_synchronous", true)
              .put(
                  "running_result_contract",
                  !result.pattern().hasRunningRows()
                      ? "progress only; RUNNING responses have total=0 and no rows"
                      : "APPEND".equals(result.pattern().updateMode())
                          ? "first response is an exact prefix of final response"
                          : "each response is a complete provisional REPLACE snapshot")
              .put("progress_response", result.pattern().name() + "-progress.json")
              .put("first_response", result.pattern().name() + "-first.json")
              .put("final_response", result.pattern().name() + "-final.json")
              .put("synchronous_response", result.pattern().name() + "-synchronous.json"));
    }
    JSONObject summary =
        new JSONObject()
            .put("documents", ROW_COUNT)
            .put("shards", SHARD_COUNT)
            .put("search_page_size", SEARCH_PAGE_SIZE)
            .put("nodes", 3)
            .put("heap_bytes_per_node", MIN_HEAP_BYTES)
            .put("patterns", patterns);
    Files.writeString(
        REPORT_DIRECTORY.resolve("summary.json"), summary.toString(2) + System.lineSeparator());
  }

  private static void report(
      QueryPattern pattern,
      long submitMillis,
      long firstProgressMillis,
      long firstUsefulMillis,
      long finalMillis,
      long synchronousMillis,
      JSONObject firstUseful) {
    System.out.printf(
        Locale.ROOT,
        "PPL_LARGE_PATTERN pattern=%s mode=%s sync_ms=%d submit_ms=%d"
            + " first_progress_ms=%d first_useful_ms=%d final_ms=%d"
            + " first_rows=%d final_rows=%d%n",
        pattern.name(),
        pattern.updateMode(),
        synchronousMillis,
        submitMillis,
        firstProgressMillis,
        firstUsefulMillis,
        finalMillis,
        firstUseful.getJSONArray("datarows").length(),
        pattern.expectedRows());
  }

  private static long elapsedMillis(long startNanos) {
    return (System.nanoTime() - startNanos) / 1_000_000;
  }

  private record QueryPattern(
      String name,
      String query,
      String updateMode,
      boolean hasRunningRows,
      boolean ordered,
      int expectedRows) {}

  private record PatternResult(
      QueryPattern pattern,
      long submitMillis,
      long firstProgressMillis,
      long firstUsefulMillis,
      long finalMillis,
      long synchronousMillis,
      int firstRows,
      int finalRows,
      int finalPageCount) {}

  private record FinalPages(JSONArray rows, List<JSONObject> responses) {}
}
