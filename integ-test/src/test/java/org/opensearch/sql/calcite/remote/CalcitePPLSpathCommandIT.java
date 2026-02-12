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
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);

    // Simple JSON docs for path-based extraction
    Request request1 = new Request("PUT", "/test_spath/_doc/1?refresh=true");
    request1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_spath/_doc/2?refresh=true");
    request2.setJsonEntity("{\"doc\": \"{\\\"n\\\": 2}\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_spath/_doc/3?refresh=true");
    request3.setJsonEntity("{\"doc\": \"{\\\"n\\\": 3}\"}");
    client().performRequest(request3);

    // Nested JSON doc for nested path extraction
    Request request4 = new Request("PUT", "/test_spath_nested/_doc/1?refresh=true");
    request4.setJsonEntity(
        "{\"doc\":"
            + " \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\",\\\"age\\\":30},\\\"active\\\":true}\"}");
    client().performRequest(request4);

    // JSON with arrays for array path extraction
    Request request5 = new Request("PUT", "/test_spath_array/_doc/1?refresh=true");
    request5.setJsonEntity(
        "{\"doc\": \"{\\\"items\\\":[{\\\"id\\\":1},{\\\"id\\\":2},{\\\"id\\\":3}]}\"}");
    client().performRequest(request5);

    // JSON with special field names for escaped path extraction
    Request request6 = new Request("PUT", "/test_spath_escape/_doc/1?refresh=true");
    request6.setJsonEntity("{\"doc\": \"{\\\"a fancy field\\\":true,\\\"a.b.c\\\":42}\"}");
    client().performRequest(request6);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | spath input=doc output=result path=n | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathPathDefaultsOutputToPath() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc path=n | fields n");
    verifySchema(result, schema("n", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathNestedPath() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_nested | spath input=doc output=name path=user.name | fields name");
    verifySchema(result, schema("name", "string"));
    verifyDataRows(result, rows("John"));
  }

  @Test
  public void testSpathMissingPath() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | spath input=doc output=result path=nonexistent | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows((Object) null), rows((Object) null), rows((Object) null));
  }

  @Test
  public void testSpathNoPathExtractAll() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath_nested | spath input=doc output=result | fields result");
    verifySchema(result, schema("result", "other"));
    // extract-all returns a map with flattened keys
    verifyDataRows(result, rows("{user.name=John, active=true, user.age=30}"));
  }

  @Test
  public void testSpathNoPathDefaultsOutputToInput() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc | fields doc");
    verifySchema(result, schema("doc", "other"));
    // output defaults to input field name, overwrites with map
    verifyDataRows(result, rows("{n=1}"), rows("{n=2}"), rows("{n=3}"));
  }

  @Test
  public void testSpathArrayIndex() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_array | spath input=doc output=first path=items{0}.id"
                + " | fields first");
    verifySchema(result, schema("first", "string"));
    verifyDataRows(result, rows("1"));
  }

  @Test
  public void testSpathArrayWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_array | spath input=doc output=all_ids path=items{}.id"
                + " | fields all_ids");
    verifySchema(result, schema("all_ids", "string"));
    verifyDataRows(result, rows("[1,2,3]"));
  }

  @Test
  public void testSpathEscapedPath() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_escape | spath input=doc output=a path=\"['a fancy field']\""
                + " | spath input=doc output=b path=\"['a.b.c']\""
                + " | fields a b");
    verifySchema(result, schema("a", "string"), schema("b", "string"));
    verifyDataRows(result, rows("true", "42"));
  }
}
