/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the PPL {@code outputlookup} command (substrate C2: one plain index per
 * lookup, weak/eventual overwrite) on the internal integ-test cluster. A lookup {@code <name>} is a
 * plain index; overwrite deletes and recreates it from the current result, append bulk-writes into
 * it, and the command returns a single {@code rows_written} count (terminal, not pass-through). A
 * name that is a #11303 filtered alias is migrated onto a dedicated plain index on overwrite.
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
        String.format(Locale.ROOT, "{\"id\":%d,\"name\":\"%s\",\"status\":%d}", id, name, status));
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
    return new JSONObject(new String(resp.getEntity().getContent().readAllBytes()))
        .getLong("count");
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
    assertTrue("lookup marker set on create", mappingJson(dest).contains("_meta"));

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

    executeQuery(String.format("source=%s | where id<3 | fields id | outputlookup %s", src, dest));
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
            "source=%s | where val<25 | fields region, host, val | outputlookup"
                + " key_field=region,host %s",
            src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));

    // Run 2: host=h1 matches (us,h1) [update in place] and (eu,h1) [insert] -> 3 distinct pairs.
    executeQuery(
        String.format(
            "source=%s | where host='h1' | fields region, host, val | outputlookup"
                + " key_field=region,host %s",
            src, dest));
    refresh(dest);
    assertEquals(
        "composite-key upsert: (us,h1) updated, (eu,h1) inserted, no duplicate",
        3L,
        docCount(dest));
  }

  // P0 #1: a key_field that is not a result field is rejected (would otherwise collapse all rows
  // into one document).
  @Test
  public void testMissingKeyFieldRejected() throws IOException {
    String src = "olkc_src_kfbad";
    seedSrc(src);
    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields id | outputlookup key_field=does_not_exist %s",
                        src, "olkc_dest_kfbad")));
    assertTrue(
        "error should name the offending key_field",
        ex.getMessage().contains("key_field") && ex.getMessage().contains("does_not_exist"));
  }

  // #1 marker guard: overwrite refuses a name that is an existing NON-lookup index (no
  // `_meta.lookup` marker), so it never clobbers unrelated data. Also covers dest == source in
  // shape.
  @Test
  public void testOverwriteRefusesForeignPlainIndex() throws IOException {
    String src = "olkc_src_coll";
    String dest = "olkc_dest_coll";
    seedSrc(src);
    // Pre-create dest as a plain, unmarked index with unrelated content.
    Request create = new Request("PUT", "/" + dest);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"other\":{\"type\":\"keyword\"}}}}");
    client().performRequest(create);
    Request doc = new Request("PUT", "/" + dest + "/_doc/x?refresh=true");
    doc.setJsonEntity("{\"other\":\"keep\"}");
    client().performRequest(doc);

    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(String.format("source=%s | fields id | outputlookup %s", src, dest)));
    assertTrue(
        "error should flag the non-lookup index", ex.getMessage().contains("non-lookup index"));
    refresh(dest);
    assertEquals("foreign index left untouched", 1L, docCount(dest));
    assertTrue("foreign content preserved", mappingJson(dest).contains("\"other\""));
  }

  // #1 marker guard: append likewise refuses a non-lookup index.
  @Test
  public void testAppendRefusesForeignPlainIndex() throws IOException {
    String src = "olkc_src_apf";
    String dest = "olkc_dest_apf";
    seedSrc(src);
    Request create = new Request("PUT", "/" + dest);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"other\":{\"type\":\"keyword\"}}}}");
    client().performRequest(create);
    Request doc = new Request("PUT", "/" + dest + "/_doc/x?refresh=true");
    doc.setJsonEntity("{\"other\":\"keep\"}");
    client().performRequest(doc);

    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields id | outputlookup append=true %s", src, dest)));
    assertTrue("append refused on non-lookup index", ex.getMessage().contains("non-lookup index"));
    refresh(dest);
    assertEquals("foreign index left untouched", 1L, docCount(dest));
  }

  // A multivalue field cannot be a key_field: it has no deterministic single-value _id encoding, so
  // it is rejected (F3). (Under C2 there is no backing index / orphan concept to assert.)
  @Test
  public void testMultivalueKeyFieldRejected() throws IOException {
    String src = "olkc_src_orph";
    Request createSrc = new Request("PUT", "/" + src);
    createSrc.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},"
            + "\"tags\":{\"type\":\"keyword\"}}}}");
    client().performRequest(createSrc);
    Request doc = new Request("PUT", "/" + src + "/_doc/1?refresh=true");
    doc.setJsonEntity("{\"id\":1,\"tags\":[\"x\",\"y\",\"z\"]}");
    client().performRequest(doc);

    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                String.format(
                    "source=%s | fields id, tags | outputlookup key_field=tags %s",
                    src, "olkc_dest_orph")));
  }

  // Backward-compat with the Dashboards data importer (#11303): a lookup created there is a
  // FILTERED alias ({term:{__lookup:<uuid>}}) over a SHARED index holding several lookups.
  // Overwrite
  // must migrate the touched lookup onto a dedicated backing by repointing the alias, and must NOT
  // delete the shared index (that would destroy the other lookups sharing it).
  @Test
  public void testOverwriteMigratesDataImporterLookupWithoutDeletingSharedIndex()
      throws IOException {
    String src = "olkc_src_mig";
    String shared = "olkc_shared_imp";
    String aliasA = "olkc_impa";
    String aliasB = "olkc_impb";
    seedSrc(src); // 3 rows: alice/bob/carol

    // Shared importer-style index: 2 docs for lookup A, 2 for lookup B, each tagged with __lookup.
    Request createShared = new Request("PUT", "/" + shared);
    createShared.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"__lookup\":{\"type\":\"keyword\"},"
            + "\"host_name\":{\"type\":\"keyword\"}}}}");
    client().performRequest(createShared);
    Request bulk = new Request("POST", "/" + shared + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n{\"__lookup\":\"A\",\"host_name\":\"a1\"}\n"
            + "{\"index\":{}}\n{\"__lookup\":\"A\",\"host_name\":\"a2\"}\n"
            + "{\"index\":{}}\n{\"__lookup\":\"B\",\"host_name\":\"b1\"}\n"
            + "{\"index\":{}}\n{\"__lookup\":\"B\",\"host_name\":\"b2\"}\n");
    client().performRequest(bulk);

    // Two filtered aliases over the shared index (the #11303 lookup shape).
    Request aliases = new Request("POST", "/_aliases");
    aliases.setJsonEntity(
        "{\"actions\":["
            + "{\"add\":{\"index\":\""
            + shared
            + "\",\"alias\":\""
            + aliasA
            + "\",\"filter\":{\"term\":{\"__lookup\":\"A\"}}}},"
            + "{\"add\":{\"index\":\""
            + shared
            + "\",\"alias\":\""
            + aliasB
            + "\",\"filter\":{\"term\":{\"__lookup\":\"B\"}}}}]}");
    client().performRequest(aliases);
    assertEquals("alias A sees only its slice before overwrite", 2L, docCount(aliasA));

    // Overwrite lookup A with a fresh 3-row result.
    JSONObject res =
        executeQuery(String.format("source=%s | fields id, name | outputlookup %s", src, aliasA));
    assertEquals(3L, rowsWritten(res));
    refresh(aliasA);

    // The shared index must still exist and lookup B must be untouched.
    Response cat =
        client().performRequest(new Request("GET", "/_cat/indices/" + shared + "?format=json"));
    JSONArray sharedIdx = new JSONArray(new String(cat.getEntity().getContent().readAllBytes()));
    assertEquals("shared importer index must NOT be deleted", 1, sharedIdx.length());
    assertEquals("other lookup B on the shared index is intact", 2L, docCount(aliasB));

    // Lookup A is migrated: the name is now a dedicated PLAIN INDEX (no alias, no __ol_ backing)
    // holding the 3 new rows. _cat/indices/<name> returns exactly that index (an alias would
    // instead resolve to a differently-named backing).
    Response migrated =
        client().performRequest(new Request("GET", "/_cat/indices/" + aliasA + "?format=json"));
    JSONArray migratedIdx =
        new JSONArray(new String(migrated.getEntity().getContent().readAllBytes()));
    assertEquals("lookup A migrated onto a single dedicated index", 1, migratedIdx.length());
    assertEquals(
        "migrated name is now a concrete index (not an alias over a backing)",
        aliasA,
        migratedIdx.getJSONObject(0).getString("index"));
    assertEquals("name A now resolves to the migrated 3-row result", 3L, docCount(aliasA));
  }

  // Append onto a shared/filtered (data-importer) lookup is refused: writing our rows there would
  // be invisible through the filtered alias or mutate a shared index. Overwrite migrates first.
  @Test
  public void testAppendToDataImporterLookupRefused() throws IOException {
    String src = "olkc_src_appmig";
    String shared = "olkc_shared_appmig";
    String alias = "olkc_impc";
    seedSrc(src);

    Request createShared = new Request("PUT", "/" + shared);
    createShared.setJsonEntity(
        "{\"mappings\":{\"properties\":{\"__lookup\":{\"type\":\"keyword\"},"
            + "\"host_name\":{\"type\":\"keyword\"}}}}");
    client().performRequest(createShared);
    Request doc = new Request("PUT", "/" + shared + "/_doc/1?refresh=true");
    doc.setJsonEntity("{\"__lookup\":\"C\",\"host_name\":\"c1\"}");
    client().performRequest(doc);
    Request aliases = new Request("POST", "/_aliases");
    aliases.setJsonEntity(
        "{\"actions\":[{\"add\":{\"index\":\""
            + shared
            + "\",\"alias\":\""
            + alias
            + "\",\"filter\":{\"term\":{\"__lookup\":\"C\"}}}}]}");
    client().performRequest(aliases);

    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields id, name | outputlookup append=true %s", src, alias)));
    assertTrue(
        "append onto a shared/filtered lookup should be refused with migration guidance",
        ex.getMessage().contains("shared or") || ex.getMessage().contains("migrate"));
    // The shared index is untouched by the refused append.
    assertEquals(1L, docCount(alias));
  }

  private void indexRegionHost(String index, int id, String region, String host, int val)
      throws IOException {
    Request req = new Request("PUT", "/" + index + "/_doc/" + id + "?refresh=true");
    req.setJsonEntity(
        String.format(
            Locale.ROOT, "{\"region\":\"%s\",\"host\":\"%s\",\"val\":%d}", region, host, val));
    client().performRequest(req);
  }
}
