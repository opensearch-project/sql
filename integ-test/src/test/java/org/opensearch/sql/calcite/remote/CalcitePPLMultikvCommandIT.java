/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Integration tests for the {@code multikv} command (fixed-schema). */
public class CalcitePPLMultikvCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Auto-header table: header on line 1, two data rows.
    if (!TestUtils.isIndexExist(client(), "test_multikv")) {
      TestUtils.createIndexByRestClient(client(), "test_multikv", null);
      Request doc = new Request("PUT", "/test_multikv/_doc/1?refresh=true");
      // raw = "CPU pctUser pctIdle\nall 5 90\n0 3 92"
      doc.setJsonEntity("{\"raw\": \"CPU pctUser pctIdle\\nall 5 90\\n0 3 92\"}");
      client().performRequest(doc);
    }

    // forceheader: line 1 is a junk banner, the real header is line 2.
    if (!TestUtils.isIndexExist(client(), "test_multikv_force")) {
      TestUtils.createIndexByRestClient(client(), "test_multikv_force", null);
      Request doc = new Request("PUT", "/test_multikv_force/_doc/1?refresh=true");
      // raw = "== report ==\nendpoint count\nfoo 1\nbar 2"
      doc.setJsonEntity("{\"raw\": \"== report ==\\nendpoint count\\nfoo 1\\nbar 2\"}");
      client().performRequest(doc);
    }

    // noheader: 3 data lines, no header row.
    if (!TestUtils.isIndexExist(client(), "test_multikv_noheader")) {
      TestUtils.createIndexByRestClient(client(), "test_multikv_noheader", null);
      Request doc = new Request("PUT", "/test_multikv_noheader/_doc/1?refresh=true");
      doc.setJsonEntity("{\"raw\": \"a 1\\nb 2\\nc 3\"}");
      client().performRequest(doc);
    }

    // structured: procs is an array of objects (nested), two elements.
    if (!TestUtils.isIndexExist(client(), "test_multikv_struct")) {
      String mapping =
          "{\"mappings\":{\"properties\":{\"procs\":{\"type\":\"nested\","
              + "\"properties\":{\"pid\":{\"type\":\"long\"},\"cpu\":{\"type\":\"double\"}}}}}}";
      TestUtils.createIndexByRestClient(client(), "test_multikv_struct", mapping);
      Request doc = new Request("PUT", "/test_multikv_struct/_doc/1?refresh=true");
      doc.setJsonEntity("{\"procs\":[{\"pid\":1,\"cpu\":0.5},{\"pid\":42,\"cpu\":9.1}]}");
      client().performRequest(doc);
    }

    // single object: meta is one object (map), not an array.
    if (!TestUtils.isIndexExist(client(), "test_multikv_obj")) {
      String mapping =
          "{\"mappings\":{\"properties\":{\"meta\":{\"properties\":"
              + "{\"owner\":{\"type\":\"keyword\"},\"role\":{\"type\":\"keyword\"}}}}}}";
      TestUtils.createIndexByRestClient(client(), "test_multikv_obj", mapping);
      Request doc = new Request("PUT", "/test_multikv_obj/_doc/1?refresh=true");
      doc.setJsonEntity("{\"meta\":{\"owner\":\"root\",\"role\":\"admin\"}}");
      client().performRequest(doc);
    }
  }

  @Test
  public void testMultikvFields() throws IOException {
    // Declared single column: auto-detected header maps pctIdle -> its column.
    JSONObject result =
        executeQuery(
            "source=test_multikv | eval _raw = raw | multikv fields pctIdle | fields pctIdle");
    verifySchema(result, schema("pctIdle", "string"));
    verifyDataRows(result, rows("90"), rows("92"));
  }

  @Test
  public void testMultikvFieldsMultipleColumns() throws IOException {
    JSONObject result =
        executeQuery("source=test_multikv | eval _raw = raw | multikv fields CPU pctIdle");
    verifySchema(result, schema("CPU", "string"), schema("pctIdle", "string"));
    verifyDataRows(result, rows("all", "90"), rows("0", "92"));
  }

  @Test
  public void testMultikvForceHeader() throws IOException {
    // forceheader=2 skips the banner and uses line 2 ("endpoint count") as the header.
    JSONObject result =
        executeQuery(
            "source=test_multikv_force | eval _raw = raw | multikv fields endpoint forceheader=2 |"
                + " fields endpoint");
    verifySchema(result, schema("endpoint", "string"));
    verifyDataRows(result, rows("foo"), rows("bar"));
  }

  @Test
  public void testMultikvNoHeaderCount() throws IOException {
    // Positional noheader, downstream only counts rows (3 data lines -> count 3).
    JSONObject result =
        executeQuery(
            "source=test_multikv_noheader | eval _raw = raw | multikv noheader=true | stats count");
    verifyDataRows(result, rows(3));
  }

  @Test
  public void testMultikvFieldEquals() throws IOException {
    // field= reads the table text directly from a field, no eval _raw needed.
    JSONObject result =
        executeQuery("source=test_multikv | multikv field=raw fields pctIdle | fields pctIdle");
    verifySchema(result, schema("pctIdle", "string"));
    verifyDataRows(result, rows("90"), rows("92"));
  }

  @Test
  public void testMultikvStructuredArrayOfObjects() throws IOException {
    // Structured input: procs is an array of objects. multikv explodes it into one row per
    // element and reads each declared column with ITEM.
    JSONObject result =
        executeQuery(
            "source=test_multikv_struct | multikv field=procs fields pid cpu"
                + " | eval p = cast(pid as int) | fields p | sort p");
    verifyDataRows(result, rows(1), rows(42));
  }

  @Test
  public void testMultikvSingleObject() throws IOException {
    // Single object (map): no row multiplication, each declared column read with ITEM -> 1 row.
    JSONObject result =
        executeQuery("source=test_multikv_obj | multikv field=meta fields owner role");
    verifyDataRows(result, rows("root", "admin"));
  }

  @Test
  public void testBareMultikvRejectedWithGuidance() throws IOException {
    // Bare auto-header multikv referencing a column downstream must be rejected at plan time
    // with guidance to declare a fields clause.
    Throwable t =
        org.junit.Assert.assertThrows(
            ResponseException.class,
            () -> executeQuery("source=test_multikv | eval _raw = raw | multikv | fields pctIdle"));
    org.junit.Assert.assertTrue(
        t.getMessage(), t.getMessage().toLowerCase().contains("fields clause"));
  }
}
