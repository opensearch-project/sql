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
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.WarningsHandler;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * End-to-end tests for the PPL {@code outputlookup} command (substrate C3) on the internal
 * integ-test cluster. Every lookup {@code <name>} is a {@code __lookup=<uuid>} slice in the shared
 * {@code .lookups} index behind a filtered alias, so reads and counts here go through the
 * per-lookup alias, which the uuid filter isolates from every other lookup sharing the index.
 * Overwrite writes a fresh slice and atomically repoints the alias; append bulks into the current
 * slice; the command returns a single {@code rows_written} count. A legacy alias over an index
 * outside {@code .lookups} is migrated onto a {@code .lookups} slice on overwrite.
 */
public class CalcitePPLOutputLookupIT extends PPLIntegTestCase {

  private static final RequestOptions PERMISSIVE = permissiveOptions();

  private static RequestOptions permissiveOptions() {
    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
    return builder.build();
  }

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

  private void refresh(String indexOrAlias) throws IOException {
    Request req = new Request("POST", "/" + indexOrAlias + "/_refresh");
    req.setOptions(PERMISSIVE);
    client().performRequest(req);
  }

  /** rows_written count returned by the outputlookup query. */
  private long rowsWritten(JSONObject result) {
    return result.getJSONArray("datarows").getJSONArray(0).getLong(0);
  }

  /** Doc count behind a lookup alias (uuid-filtered) or a concrete index. */
  private long docCount(String indexOrAlias) throws IOException {
    Request req = new Request("GET", "/" + indexOrAlias + "/_count");
    req.setOptions(PERMISSIVE);
    Response resp = client().performRequest(req);
    return new JSONObject(new String(resp.getEntity().getContent().readAllBytes()))
        .getLong("count");
  }

  /** _source of the first hit behind a lookup alias or index. */
  private JSONObject firstHitSource(String indexOrAlias) throws IOException {
    Request req = new Request("GET", "/" + indexOrAlias + "/_search");
    req.setOptions(PERMISSIVE);
    Response resp = client().performRequest(req);
    JSONObject json = new JSONObject(new String(resp.getEntity().getContent().readAllBytes()));
    return json.getJSONObject("hits")
        .getJSONArray("hits")
        .getJSONObject(0)
        .getJSONObject("_source");
  }

  /** The single index a filtered lookup alias currently points at. */
  private String aliasTargetIndex(String alias) throws IOException {
    Request req = new Request("GET", "/_alias/" + alias);
    req.setOptions(PERMISSIVE);
    Response resp = client().performRequest(req);
    JSONObject json = new JSONObject(new String(resp.getEntity().getContent().readAllBytes()));
    return json.keys().next();
  }

  private long catIndexCount(String index) throws IOException {
    Request req = new Request("GET", "/_cat/indices/" + index + "?format=json");
    req.setOptions(PERMISSIVE);
    Response resp = client().performRequest(req);
    return new JSONArray(new String(resp.getEntity().getContent().readAllBytes())).length();
  }

  // Overwrite returns the count, repoints the alias to a fresh slice, and the new slice holds only
  // the current result's fields (absent fields are not in the new slice's documents).
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
    assertTrue("name present in the first slice", firstHitSource(dest).has("name"));

    JSONObject r2 =
        executeQuery(
            String.format("source=%s | where id=1 | fields id | outputlookup %s", src, dest));
    assertEquals(1L, rowsWritten(r2));
    refresh(dest);
    assertEquals("overwrite repointed the alias to the new 1-row slice", 1L, docCount(dest));
    assertFalse("absent field not in the new slice's document", firstHitSource(dest).has("name"));
  }

  // append keeps existing rows AND keeps a new field (OpenSearch is schemaless, .lookups template).
  @Test
  public void testAppendKeepsNewField() throws IOException {
    String src = "olkc_src_ap";
    String dest = "olkc_dest_ap";
    seedSrc(src);

    executeQuery(String.format("source=%s | where id<3 | fields id | outputlookup %s", src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));

    JSONObject r =
        executeQuery(
            String.format(
                "source=%s | where id=3 | fields id, name | outputlookup append=true %s",
                src, dest));
    assertEquals(1L, rowsWritten(r));
    refresh(dest);
    assertEquals("append adds into the same slice without clearing", 3L, docCount(dest));
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

  // Default override_if_empty=true clears the lookup on an empty result (repoints to an empty
  // slice).
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

  // key_field upserts by id (implies append) — re-run updates in place within the slice, no
  // duplicates.
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

  // The operator ceiling plugins.ppl.outputlookup.max_rows fails loud (does not truncate) and
  // writes nothing when the input exceeds it.
  @Test
  public void testMaxRowsSettingRejectsExceeding() throws IOException {
    String src = "olkc_src_cap";
    String dest = "olkc_dest_cap";
    seedSrc(src); // 3 rows
    setMaxRows("2");
    try {
      ResponseException ex =
          assertThrows(
              ResponseException.class,
              () ->
                  executeQuery(
                      String.format("source=%s | fields id | outputlookup %s", src, dest)));
      assertTrue(
          "error names the max_rows ceiling",
          ex.getMessage().contains("plugins.ppl.outputlookup.max_rows"));
      // fail-loud, not partial: the destination never came into existence
      Request head = new Request("GET", "/_alias/" + dest);
      head.setOptions(PERMISSIVE);
      ResponseException notFound =
          assertThrows(ResponseException.class, () -> client().performRequest(head));
      assertEquals(
          "no alias created on a rejected write",
          404,
          notFound.getResponse().getStatusLine().getStatusCode());
    } finally {
      setMaxRows(null);
    }
  }

  private void setMaxRows(String value) throws IOException {
    Request req = new Request("PUT", "/_cluster/settings");
    String v = value == null ? "null" : "\"" + value + "\"";
    req.setJsonEntity("{\"transient\":{\"plugins.ppl.outputlookup.max_rows\":" + v + "}}");
    client().performRequest(req);
  }

  // A multivalue field is preserved as a native array.
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

    JSONArray tags = firstHitSource(dest).getJSONArray("tags");
    assertEquals("multivalue preserved as a native array", 3, tags.length());
  }

  // Source larger than the 10k result window is written in full into the slice (no truncation).
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
    assertEquals("the slice holds all rows", (long) total, docCount(dest));
  }

  // A composite (multi-field) key_field upserts by the pair, so re-running updates matching pairs
  // in place with no duplicates.
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

    executeQuery(
        String.format(
            "source=%s | where val<25 | fields region, host, val | outputlookup"
                + " key_field=region,host %s",
            src, dest));
    refresh(dest);
    assertEquals(2L, docCount(dest));

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

  // A key_field that is not a result field is rejected at plan time (would otherwise collapse all
  // rows into one document).
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

  // Overwrite refuses a name that is an existing concrete index (a concrete index cannot share a
  // name with the lookup alias), so it never clobbers unrelated data.
  @Test
  public void testOverwriteRefusesConcreteSameNameIndex() throws IOException {
    String src = "olkc_src_coll";
    String dest = "olkc_dest_coll";
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
                executeQuery(String.format("source=%s | fields id | outputlookup %s", src, dest)));
    assertTrue("error should flag the concrete index", ex.getMessage().contains("concrete index"));
    refresh(dest);
    assertEquals("concrete index left untouched", 1L, docCount(dest));
    assertTrue("its content preserved", firstHitSource(dest).has("other"));
  }

  // Append likewise refuses a concrete same-name index.
  @Test
  public void testAppendRefusesConcreteSameNameIndex() throws IOException {
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
    assertTrue("append refused on concrete index", ex.getMessage().contains("concrete index"));
    refresh(dest);
    assertEquals("concrete index left untouched", 1L, docCount(dest));
  }

  // A multivalue field cannot be a key_field: it has no deterministic single-value _id encoding.
  @Test
  public void testMultivalueKeyFieldRejected() throws IOException {
    String src = "olkc_src_mvk";
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
                    src, "olkc_dest_mvk")));
  }

  // Two lookups written into the shared .lookups index are isolated by their __lookup uuid: each
  // alias sees only its own slice, and a shared field name (name) coexists without collision.
  @Test
  public void testTwoLookupsShareLookupsIndexIsolatedByUuid() throws IOException {
    String src = "olkc_src_iso";
    String lka = "olkc_iso_a";
    String lkb = "olkc_iso_b";
    seedSrc(src);

    executeQuery(String.format("source=%s | fields id, name | outputlookup %s", src, lka));
    executeQuery(
        String.format("source=%s | where id=1 | fields id, name | outputlookup %s", src, lkb));
    refresh(lka);
    refresh(lkb);

    assertEquals("lookup A sees only its 3-row slice", 3L, docCount(lka));
    assertEquals("lookup B sees only its 1-row slice", 1L, docCount(lkb));
    assertTrue("shared field name present in A", firstHitSource(lka).has("name"));
    assertTrue("shared field name present in B", firstHitSource(lkb).has("name"));
  }

  // Backward-compat: a legacy data-importer lookup is a filtered alias over an index OUTSIDE
  // .lookups. Overwrite migrates it onto a fresh .lookups slice by repointing the alias, and must
  // NOT delete the legacy shared index (other lookups still use it).
  @Test
  public void testOverwriteMigratesLegacyFilteredAliasOntoLookups() throws IOException {
    String src = "olkc_src_mig";
    String shared = "olkc_shared_imp";
    String aliasA = "olkc_impa";
    String aliasB = "olkc_impb";
    seedSrc(src); // 3 rows

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

    JSONObject res =
        executeQuery(String.format("source=%s | fields id, name | outputlookup %s", src, aliasA));
    assertEquals(3L, rowsWritten(res));
    refresh(aliasA);

    assertEquals("legacy shared index must NOT be deleted", 1L, catIndexCount(shared));
    assertEquals("other lookup B on the shared index is intact", 2L, docCount(aliasB));
    assertEquals("alias A repointed onto .lookups", ".lookups", aliasTargetIndex(aliasA));
    assertEquals("alias A now resolves to the migrated 3-row slice", 3L, docCount(aliasA));
  }

  // Append onto a non-filtered alias is refused: it is not a recognized lookup.
  @Test
  public void testAppendRefusedOnNonFilteredAlias() throws IOException {
    String src = "olkc_src_nfa";
    String backing = "olkc_nfa_backing";
    String alias = "olkc_nfa_alias";
    seedSrc(src);
    Request create = new Request("PUT", "/" + backing);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"x\":{\"type\":\"keyword\"}}}}");
    client().performRequest(create);
    Request aliases = new Request("POST", "/_aliases");
    aliases.setJsonEntity(
        "{\"actions\":[{\"add\":{\"index\":\"" + backing + "\",\"alias\":\"" + alias + "\"}}]}");
    client().performRequest(aliases);

    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields id | outputlookup append=true %s", src, alias)));
    assertTrue(
        "append onto a non-filtered alias is refused",
        ex.getMessage().contains("non-filtered alias"));
  }

  // Append onto a filtered alias whose filter carries no __lookup discriminant is refused: it is
  // not a recognized lookup slice.
  @Test
  public void testAppendRefusedOnAliasWithoutLookupDiscriminant() throws IOException {
    String src = "olkc_src_nod";
    String backing = "olkc_nod_backing";
    String alias = "olkc_nod_alias";
    seedSrc(src);
    Request create = new Request("PUT", "/" + backing);
    create.setJsonEntity("{\"mappings\":{\"properties\":{\"kind\":{\"type\":\"keyword\"}}}}");
    client().performRequest(create);
    Request aliases = new Request("POST", "/_aliases");
    aliases.setJsonEntity(
        "{\"actions\":[{\"add\":{\"index\":\""
            + backing
            + "\",\"alias\":\""
            + alias
            + "\",\"filter\":{\"term\":{\"kind\":\"x\"}}}}]}");
    client().performRequest(aliases);

    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | fields id | outputlookup append=true %s", src, alias)));
    assertTrue(
        "append onto an alias without a __lookup discriminant is refused",
        ex.getMessage().contains("__lookup discriminant"));
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
