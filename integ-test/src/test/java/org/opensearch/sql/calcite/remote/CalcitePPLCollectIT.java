/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the PPL {@code collect} command (pass-through write spool). Verifies the
 * implemented Scannable (pushable) path: rows are appended to a pre-existing destination index and
 * also pass through to the result set, large sources stream past the result window, and the safety
 * refusals (missing / dot-prefixed destination) fire at plan time.
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

    refresh(dest);
    assertEquals("all source rows must be appended to the destination", 3L, count(dest));
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

    refresh(dest);
    assertEquals("only the filtered rows are written", 2L, count(dest));
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

    executeQuery(String.format("source=%s | fields id | collect index=%s", src, dest));
    refresh(dest);
    assertEquals(
        "collect must stream the full source past the result window to the destination",
        (long) total,
        count(dest));
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

    refresh(dest);
    // The written documents carry the stamped source/marker fields.
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
}
