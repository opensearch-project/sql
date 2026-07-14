/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the clean PPL {@code outputlookup} command on the internal integ-test
 * cluster. A lookup is an alias over a backing index; overwrite writes a fresh backing and
 * atomically repoints the alias, append writes the current backing, and the command returns a
 * single {@code rows_written} count (terminal, not pass-through). Reads/asserts go through REST so
 * they exercise the alias.
 */
public class CalcitePPLOutputLookupIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  private void seedSrc(String src) throws IOException {
    Request create = new Request("PUT", "/" + src);
    create.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
            + "\"name\":{\"type\":\"keyword\"},\"status\":{\"type\":\"integer\"}}}}");
    client().performRequest(create);
    indexDoc(src, 1, "alice", 200);
    indexDoc(src, 2, "bob", 500);
    indexDoc(src, 3, "carol", 404);
  }

  private void indexDoc(String index, int id, String name, int status) throws IOException {
    Request req = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
    req.setJsonEntity(
        String.format(
            Locale.ROOT, "{\"id\":%d,\"name\":\"%s\",\"status\":%d}", id, name, status));
    client().performRequest(req);
  }

  private void refresh(String index) throws IOException {
    client().performRequest(new Request("POST", "/" + index + "/_refresh"));
  }

  /** rows_written count returned by the outputlookup query. */
  private long rowsWritten(JSONObject result) {
    return result.getJSONArray("datarows").getJSONArray(0).getLong(0);
  }

  private long docCount(String indexOrAlias) throws IOException {
    Response resp = client().performRequest(new Request("GET", "/" + indexOrAlias + "/_count"));
    return new JSONObject(new String(resp.getEntity().getContent().readAllBytes())).getLong("count");
  }

  private String mappingJson(String indexOrAlias) throws IOException {
    Response resp = client().performRequest(new Request("GET", "/" + indexOrAlias + "/_mapping"));
    return new String(resp.getEntity().getContent().readAllBytes());
  }

  // Overwrite returns the count, replaces rows, and drops fields absent from the new result.
  @Test
  public void testOverwriteReturnsCountReplacesAndDropsAbsentFields() throws IOException {
    String src = "olkc_src_ow";
    String dest = "olkc_dest_ow";
    seedSrc(src);

    JSONObject r1 =
        executeQuery(String.format("source=%s | fields id, name | outputlookup %s", src, dest));
    assertEquals("returns rows_written count", 3L, rowsWritten(r1));
    refresh(dest);
    assertEquals(3L, docCount(dest));
    assertTrue("name present after first write", mappingJson(dest).contains("\"name\""));

    JSONObject r2 =
        executeQuery(
            String.format("source=%s | where id=1 | fields id | outputlookup %s", src, dest));
    assertEquals(1L, rowsWritten(r2));
    refresh(dest);
    assertEquals("overwrite replaced all rows", 1L, docCount(dest));
    assertFalse("absent field dropped by fresh backing", mappingJson(dest).contains("\"name\""));
  }

  // A6#1: append keeps existing rows AND keeps a new field (OpenSearch is schemaless).
  @Test
  public void testAppendKeepsNewFieldUnlikeSplunk() throws IOException {
    String src = "olkc_src_ap";
    String dest = "olkc_dest_ap";
    seedSrc(src);

    executeQuery(
        String.format("source=%s | where id<3 | fields id | outputlookup %s", src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));
    assertFalse(mappingJson(dest).contains("\"name\""));

    JSONObject r =
        executeQuery(
            String.format(
                "source=%s | where id=3 | fields id, name | outputlookup append=true %s",
                src, dest));
    assertEquals(1L, rowsWritten(r));
    refresh(dest);
    assertEquals("append adds without clearing", 3L, docCount(dest));
    assertTrue("new field kept (schemaless)", mappingJson(dest).contains("\"name\""));
  }

  // override_if_empty=false leaves the existing lookup intact on an empty result.
  @Test
  public void testOverrideIfEmptyFalseKeeps() throws IOException {
    String src = "olkc_src_oif";
    String dest = "olkc_dest_oif";
    seedSrc(src);

    executeQuery(String.format("source=%s | fields id | outputlookup %s", src, dest));
    refresh(dest);
    assertEquals(3L, docCount(dest));

    JSONObject r =
        executeQuery(
            String.format(
                "source=%s | where id=999 | outputlookup override_if_empty=false %s", src, dest));
    assertEquals("empty write reports 0", 0L, rowsWritten(r));
    refresh(dest);
    assertEquals("empty guard kept existing lookup", 3L, docCount(dest));
  }

  // Default override_if_empty=true clears the lookup on an empty result.
  @Test
  public void testOverrideIfEmptyTrueClears() throws IOException {
    String src = "olkc_src_oit";
    String dest = "olkc_dest_oit";
    seedSrc(src);

    executeQuery(String.format("source=%s | fields id | outputlookup %s", src, dest));
    refresh(dest);
    assertEquals(3L, docCount(dest));

    executeQuery(String.format("source=%s | where id=999 | outputlookup %s", src, dest));
    refresh(dest);
    assertEquals("empty result cleared the lookup", 0L, docCount(dest));
  }

  // key_field upserts by id (append-default) — re-run updates in place, no duplicates.
  @Test
  public void testKeyFieldUpsertNoDuplicate() throws IOException {
    String src = "olkc_src_kf";
    String dest = "olkc_dest_kf";
    seedSrc(src);

    executeQuery(
        String.format(
            "source=%s | where id<3 | fields id, name | outputlookup key_field=id %s", src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));

    executeQuery(
        String.format(
            "source=%s | where id=1 or id=3 | fields id, name | outputlookup key_field=id %s",
            src, dest));
    refresh(dest);
    assertEquals("upsert updates in place, no duplicate", 3L, docCount(dest));
  }

  // max caps rows written (and the returned count).
  @Test
  public void testMaxCapsRows() throws IOException {
    String src = "olkc_src_max";
    String dest = "olkc_dest_max";
    seedSrc(src);

    JSONObject r =
        executeQuery(String.format("source=%s | fields id | outputlookup max=2 %s", src, dest));
    assertEquals("max caps the count", 2L, rowsWritten(r));
    refresh(dest);
    assertEquals("max caps written rows", 2L, docCount(dest));
  }

  // A6#2: a multivalue field is preserved as a native array.
  @Test
  public void testMultivaluePreservedAsArray() throws IOException {
    String src = "olkc_src_mv";
    String dest = "olkc_dest_mv";
    Request createSrc = new Request("PUT", "/" + src);
    createSrc.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
            + "\"tags\":{\"type\":\"keyword\"}}}}");
    client().performRequest(createSrc);
    Request doc = new Request("PUT", "/" + src + "/_doc/1?refresh=true");
    doc.setJsonEntity("{\"id\":1,\"tags\":[\"x\",\"y\",\"z\"]}");
    client().performRequest(doc);

    executeQuery(String.format("source=%s | fields id, tags | outputlookup %s", src, dest));
    refresh(dest);

    Response resp = client().performRequest(new Request("GET", "/" + dest + "/_search"));
    JSONObject json = new JSONObject(new String(resp.getEntity().getContent().readAllBytes()));
    JSONObject source =
        json.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source");
    assertTrue("tags present", source.has("tags"));
    JSONArray tags = source.getJSONArray("tags");
    assertEquals("multivalue preserved as a native array", 3, tags.length());
  }

  // B: source larger than the 10k result window is written in full (no silent truncation).
  @Test
  public void testLargeSourceWritesPastQuerySizeLimit() throws IOException {
    String src = "olkc_src_big";
    String dest = "olkc_dest_big";
    Request create = new Request("PUT", "/" + src);
    create.setJsonEntity(
        "{\"settings\":{\"index.max_result_window\":10000},"
            + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"}}}}");
    client().performRequest(create);

    int total = 12000; // > default max_result_window (10000): only PIT paging reaches all rows
    StringBuilder bulk = new StringBuilder();
    for (int i = 1; i <= total; i++) {
      bulk.append(
          String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":%d}}%n", src, i));
      bulk.append(String.format(Locale.ROOT, "{\"id\":%d}%n", i));
      if (i % 3000 == 0 || i == total) {
        Request r = new Request("POST", "/_bulk?refresh=true");
        r.setJsonEntity(bulk.toString());
        client().performRequest(r);
        bulk.setLength(0);
      }
    }

    JSONObject res =
        executeQuery(String.format("source=%s | fields id | outputlookup %s", src, dest));
    assertEquals("writes all rows past the 10k window", (long) total, rowsWritten(res));
    refresh(dest);
    assertEquals("destination holds all rows", (long) total, docCount(dest));
  }

  // A7 idempotency end-to-end: a composite (multi-field) key_field upserts by the pair, so
  // re-running updates matching pairs in place with no duplicates.
  @Test
  public void testMultiFieldKeyFieldUpsertNoDuplicate() throws IOException {
    String src = "olkc_src_mkf";
    String dest = "olkc_dest_mkf";
    Request create = new Request("PUT", "/" + src);
    create.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"region\":{\"type\":\"keyword\"},"
            + "\"host\":{\"type\":\"keyword\"},\"val\":{\"type\":\"integer\"}}}}");
    client().performRequest(create);
    indexRegionHost(src, 1, "us", "h1", 10);
    indexRegionHost(src, 2, "us", "h2", 20);
    indexRegionHost(src, 3, "eu", "h1", 30);

    // Run 1: upsert (us,h1) and (us,h2) -> 2 pairs.
    executeQuery(
        String.format(
            "source=%s | where val<25 | fields region, host, val | outputlookup key_field=region,host %s",
            src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));

    // Run 2: host=h1 matches (us,h1) [update in place] and (eu,h1) [insert] -> 3 distinct pairs.
    executeQuery(
        String.format(
            "source=%s | where host='h1' | fields region, host, val | outputlookup key_field=region,host %s",
            src, dest));
    refresh(dest);
    assertEquals("composite-key upsert: (us,h1) updated, (eu,h1) inserted, no duplicate", 3L, docCount(dest));
  }

  private void indexRegionHost(String index, int id, String region, String host, int val)
      throws IOException {
    Request req = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
    req.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\"region\":\"%s\",\"host\":\"%s\",\"val\":%d}",
            region,
            host,
            val));
    client().performRequest(req);
  }
}
