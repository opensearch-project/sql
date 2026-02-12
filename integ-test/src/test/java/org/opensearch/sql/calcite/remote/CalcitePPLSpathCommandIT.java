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

    // Auto-extract mode: one doc field per flatten rule
    Request autoExtractDoc = new Request("PUT", "/test_spath_auto/_doc/1?refresh=true");
    autoExtractDoc.setJsonEntity(
        "{\"nested_doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\"}}\","
            + " \"array_doc\": \"{\\\"tags\\\":[\\\"java\\\",\\\"sql\\\"]}\","
            + " \"merge_doc\": \"{\\\"a\\\":{\\\"b\\\":1},\\\"a.b\\\":2}\","
            + " \"stringify_doc\":"
            + " \"{\\\"n\\\":30,\\\"b\\\":true,\\\"x\\\":null}\"}");
    client().performRequest(autoExtractDoc);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | spath input=doc output=result path=n | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathAutoExtract() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc");
    verifySchema(result, schema("doc", "struct"));
  }

  @Test
  public void testSpathAutoExtractWithOutput() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc output=result");
    verifySchema(result, schema("doc", "string"), schema("result", "struct"));
  }

  @Test
  public void testSpathAutoExtractNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=nested_doc output=result | fields result");

    // Nested objects flatten to dotted keys: user.name
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"user.name\":\"John\"}")));
  }

  @Test
  public void testSpathAutoExtractArraySuffix() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=array_doc output=result | fields result");

    // Arrays use {} suffix: tags{}
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"tags{}\":[\"java\",\"sql\"]}")));
  }

  @Test
  public void testSpathAutoExtractDuplicateKeysMerge() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=merge_doc output=result | fields result");

    // Duplicate logical keys merge into arrays: a.b from nested and dotted key
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"a.b\":[\"1\",\"2\"]}")));
  }

  @Test
  public void testSpathAutoExtractStringifyAndNull() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=stringify_doc output=result | fields result");

    // All values stringified, null preserved
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"n\":\"30\",\"b\":\"true\",\"x\":\"null\"}")));
  }
}
