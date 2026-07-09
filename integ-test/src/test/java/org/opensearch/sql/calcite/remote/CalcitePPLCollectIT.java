/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the PPL {@code collect} command (async materialization, Seam A). Verifies
 * the async contract: the foreground runs a read-only preview of the upstream pipeline (the {@code
 * collect} clause stripped, capped at {@code QUERY_SIZE_LIMIT}) and returns those preview rows plus
 * a background {@code task_id}, while a background CollectMaterialize task re-runs the full
 * pipeline (including {@code collect}) and streams the write into the pre-existing destination
 * (fire-and-forget, eventually consistent). Durability assertions poll until the background task
 * makes the rows visible; write failures are observed via the {@code _tasks} API. Large sources
 * stream past the result window via PIT; the safety refusals (missing or dot-prefixed destination)
 * fire synchronously at plan time.
 */
public class CalcitePPLCollectIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  /** Creates an index with a minimal {id:int, name:keyword, status:int} mapping. */
  private void createIndex(String index) throws IOException {
    Request req = new Request("PUT", "/" + index);
    req.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
            + "\"name\":{\"type\":\"keyword\"},\"status\":{\"type\":\"integer\"}}}}");
    client().performRequest(req);
  }

  /** Same mapping as {@link #createIndex} but with a lowered {@code index.max_result_window}. */
  private void createIndexWithWindow(String index, int maxResultWindow) throws IOException {
    Request req = new Request("PUT", "/" + index);
    req.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"settings\":{\"index.max_result_window\":%d},"
                + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
                + "\"name\":{\"type\":\"keyword\"},\"status\":{\"type\":\"integer\"}}}}",
            maxResultWindow));
    client().performRequest(req);
  }

  private void indexDoc(String index, int id, String name, int status) throws IOException {
    Request req = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
    req.setJsonEntity(
        String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"%s\",\"status\":%d}", id, name, status));
    client().performRequest(req);
  }

  private void refresh(String index) throws IOException {
    client().performRequest(new Request("POST", "/" + index + "/_refresh"));
  }

  private long count(String index) throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | stats count() as c", index));
    return result.getJSONArray("datarows").getJSONArray(0).getLong(0);
  }

  @Test
  public void testCollectSmallRoundTripAndPassThrough() throws IOException {
    String src = "collect_src_small";
    String dest = "collect_dest_small";
    createIndex(src);
    createIndex(dest);
    indexDoc(src, 1, "a", 200);
    indexDoc(src, 2, "b", 500);
    indexDoc(src, 3, "c", 404);

    // collect appends to dest AND passes the rows through to the result set.
    JSONObject result =
        executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
    assertEquals(
        "collect must pass its input rows through unchanged",
        3,
        result.getJSONArray("datarows").length());

    // Async: the background task ingests after the query returns, so poll until visible.
    awaitCount(dest, 3L);
  }

  @Test
  public void testCollectWithFilterFullScope() throws IOException {
    String src = "collect_src_filtered";
    String dest = "collect_dest_filtered";
    createIndex(src);
    createIndex(dest);
    indexDoc(src, 1, "a", 200);
    indexDoc(src, 2, "b", 500);
    indexDoc(src, 3, "c", 503);

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where status >= 500 | fields id, status | collect index=%s",
                src, dest));
    assertEquals(2, result.getJSONArray("datarows").length());

    awaitCount(dest, 2L);
  }

  @Test
  public void testCollectStreamsPastResultWindow() throws IOException {
    String src = "collect_src_big";
    String dest = "collect_dest_big";
    createIndex(src);
    createIndex(dest);
    // 10500 > default max_result_window (10000): verifies collect streams the full source past it.
    int total = 10500;
    StringBuilder bulk = new StringBuilder();
    for (int i = 1; i <= total; i++) {
      bulk.append(
          String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":%d}}%n", src, i));
      bulk.append(
          String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"n%d\",\"status\":200}%n", i, i));
      if (i % 2000 == 0 || i == total) {
        Request req = new Request("POST", "/_bulk?refresh=true");
        req.setJsonEntity(bulk.toString());
        client().performRequest(req);
        bulk.setLength(0);
      }
    }

    // Seam A split: the foreground preview is capped below the full source volume, while the
    // background materialization writes every row past the result window.
    JSONObject result =
        executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
    assertTrue(
        "the foreground preview must be capped below the full source volume",
        result.getJSONArray("datarows").length() < total);
    awaitCount(dest, (long) total);
  }

  @Test
  public void testCollectStreamsViaPitPastLoweredResultWindow() throws IOException {
    String src = "collect_src_pit";
    String dest = "collect_dest_pit";
    // Source window capped at 100: a single (non-PIT) search can return at most 100 hits. Returning
    // all 350 preview rows requires the foreground preview read to PIT and search_after page the
    // source, and the background materialization likewise pages to write all 350. This proves the
    // streaming mechanism directly, at low volume.
    createIndexWithWindow(src, 100);
    createIndex(dest);
    int total = 350;
    StringBuilder bulk = new StringBuilder();
    for (int i = 1; i <= total; i++) {
      bulk.append(
          String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":%d}}%n", src, i));
      bulk.append(
          String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"n%d\",\"status\":200}%n", i, i));
    }
    Request req = new Request("POST", "/_bulk?refresh=true");
    req.setJsonEntity(bulk.toString());
    client().performRequest(req);

    // Pass-through returns every streamed row (QUERY_SIZE_LIMIT default 10000 > 350).
    JSONObject result =
        executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
    assertEquals(
        "collect must pass through every PIT-paged row",
        total,
        result.getJSONArray("datarows").length());

    awaitCount(dest, (long) total);
  }

  @Test
  public void testCollectRenewsPitLeaseAcrossLongDrain() throws IOException {
    String src = "collect_src_keepalive";
    String dest = "collect_dest_keepalive";
    // window=50 forces many PIT pages; the background drain does one search and one bulk write per
    // page, so the whole walk spans many round-trips and comfortably exceeds a 1s creation-time
    // lease.
    createIndexWithWindow(src, 50);
    createIndex(dest);
    int total = 8000;
    for (int start = 1; start <= total; start += 2000) {
      StringBuilder bulk = new StringBuilder();
      for (int i = start; i < start + 2000 && i <= total; i++) {
        bulk.append(
            String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":%d}}%n", src, i));
        bulk.append(
            String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"n%d\",\"status\":200}%n", i, i));
      }
      Request bulkReq = new Request("POST", "/_bulk?refresh=true");
      bulkReq.setJsonEntity(bulk.toString());
      client().performRequest(bulkReq);
    }

    // Shrink the cursor keep-alive so the PIT's creation-time lease would expire mid-drain.
    // Only the per-page setKeepAlive renewal keeps the PIT alive to completion; without it the
    // walk fails with a missing search context. 1s is far longer than any single page's
    // fetch+write gap, so the renewing (correct) path never flakes on it.
    setCursorKeepAlive("1s");
    try {
      executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
      awaitCount(dest, (long) total);
    } finally {
      setCursorKeepAlive(null);
    }
  }

  private void setCursorKeepAlive(String value) throws IOException {
    Request req = new Request("PUT", "/_cluster/settings");
    String v = value == null ? "null" : "\"" + value + "\"";
    req.setJsonEntity(
        String.format(Locale.ROOT, "{\"transient\":{\"plugins.sql.cursor.keep_alive\":%s}}", v));
    client().performRequest(req);
  }

  @Test
  public void testCollectSurfacesNonRetriableBulkFailure() throws IOException {
    String src = "collect_src_fail";
    String dest = "collect_dest_fail";
    createIndex(src);
    indexDoc(src, 1, "notanumber", 200);
    // dest.name is typed integer, so writing the source's keyword "name" is a non-429
    // mapper_parsing failure. collect is fire-and-forget, so the failure does not reach the
    // foreground; it is recorded on the background task and observable via the _tasks API.
    Request destReq = new Request("PUT", "/" + dest);
    destReq.setJsonEntity("{\"mappings\":{\"properties\":{\"name\":{\"type\":\"integer\"}}}}");
    client().performRequest(destReq);

    JSONObject result =
        executeQuery(String.format("source=%s | fields name | collect index=%s", src, dest));
    String taskId = result.getString("task_id");
    awaitTaskFailure(taskId);
  }

  /**
   * Polls GET _tasks/&lt;taskId&gt; until the task completes, then asserts it recorded an error.
   */
  private void awaitTaskFailure(String taskId) throws IOException {
    long deadline = System.currentTimeMillis() + 30_000L;
    while (System.currentTimeMillis() < deadline) {
      Request req = new Request("GET", "/_tasks/" + taskId);
      String body =
          new String(client().performRequest(req).getEntity().getContent().readAllBytes());
      JSONObject json = new JSONObject(body);
      if (json.optBoolean("completed", false)) {
        assertTrue(
            "the background collect task must record the non-retriable write failure",
            json.has("error"));
        return;
      }
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    fail("collect background task did not complete within timeout");
  }

  @Test
  public void testCollectMissingDestinationFails() throws IOException {
    String src = "collect_src_missing";
    createIndex(src);
    indexDoc(src, 1, "a", 200);
    Class<? extends Exception> expected =
        isStandaloneTest() ? RuntimeException.class : ResponseException.class;
    Exception e =
        assertThrows(
            expected,
            () ->
                executeQuery(String.format("source=%s | collect index=collect_no_such_dest", src)));
    verifyErrorMessageContains(e, "collect_no_such_dest");
  }

  @Test
  public void testCollectDotIndexRefused() throws IOException {
    String src = "collect_src_dot";
    createIndex(src);
    indexDoc(src, 1, "a", 200);
    Class<? extends Exception> expected =
        isStandaloneTest() ? RuntimeException.class : ResponseException.class;
    Exception e =
        assertThrows(
            expected,
            () -> executeQuery(String.format("source=%s | collect index=.system_dest", src)));
    verifyErrorMessageContains(e, "system or hidden");
  }

  @Test
  public void testCollectStampsOptionFields() throws IOException {
    String src = "collect_src_stamp";
    String dest = "collect_dest_stamp";
    createIndex(src);
    createIndex(dest);
    indexDoc(src, 1, "a", 200);
    indexDoc(src, 2, "b", 200);

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields id | collect index=%s source='app1' marker='m1'", src, dest));
    assertEquals(
        "collect must pass its input rows through", 2, result.getJSONArray("datarows").length());

    // Async: wait until all rows land, then verify the stamped fields on the written docs.
    awaitCount(dest, 2L);
    JSONObject stamped =
        executeQuery(
            String.format(
                "source=%s | where source='app1' and marker='m1' | stats count() as c", dest));
    assertEquals(
        "written documents must carry the stamped source/marker fields",
        2L,
        stamped.getJSONArray("datarows").getJSONArray(0).getLong(0));
  }

  @Test
  public void testCollectTestmodeDoesNotWrite() throws IOException {
    String src = "collect_src_testmode";
    String dest = "collect_dest_testmode";
    createIndex(src);
    createIndex(dest);
    indexDoc(src, 1, "a", 200);
    indexDoc(src, 2, "b", 200);
    indexDoc(src, 3, "c", 200);

    // testmode is a dry-run: rows pass through (with any stamps) but nothing is written.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields id | collect index=%s testmode=true source='app1'", src, dest));
    assertEquals(
        "testmode must still pass rows through", 3, result.getJSONArray("datarows").length());

    refresh(dest);
    assertEquals("testmode must NOT write to the destination", 0L, count(dest));
  }

  @Test
  public void testCollectAsyncReturnsPreviewAndTaskIdThenEventuallyVisible() throws IOException {
    String src = "collect_src_async";
    String dest = "collect_dest_async";
    createIndex(src);
    createIndex(dest);
    indexDoc(src, 1, "a", 200);
    indexDoc(src, 2, "b", 200);
    indexDoc(src, 3, "c", 200);

    // Default (no wait_for_completion) = async materialization: the foreground returns the capped
    // preview rows AND a top-level task_id (envelope A); the destination is written by the
    // background CollectMaterialize task, so the rows are only *eventually* visible.
    JSONObject result =
        executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
    assertEquals(
        "async collect must still pass the preview rows through",
        3,
        result.getJSONArray("datarows").length());
    assertTrue(
        "async collect must surface a task_id in the response metadata", result.has("task_id"));
    assertFalse("task_id must be non-empty", result.getString("task_id").isEmpty());

    // Eventually consistent: poll until the background task has ingested all rows.
    awaitCount(dest, 3L);
  }

  /** Polls (refresh + count) until the destination reaches {@code expected} or times out. */
  private void awaitCount(String index, long expected) throws IOException {
    long deadline = System.currentTimeMillis() + 30_000L;
    long actual = -1L;
    while (System.currentTimeMillis() < deadline) {
      refresh(index);
      actual = count(index);
      if (actual == expected) {
        return;
      }
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    assertEquals(
        "async collect must eventually make all rows visible in the destination", expected, actual);
  }
}
