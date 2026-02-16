/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
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

    // Auto-extract mode: flatten rules and edge cases (empty, malformed)
    Request autoExtractDoc = new Request("PUT", "/test_spath_auto/_doc/1?refresh=true");
    autoExtractDoc.setJsonEntity(
        "{\"nested_doc\": \"{\\\"user\\\":{\\\"name\\\":\\\"John\\\"}}\","
            + " \"array_doc\": \"{\\\"tags\\\":[\\\"java\\\",\\\"sql\\\"]}\","
            + " \"merge_doc\": \"{\\\"a\\\":{\\\"b\\\":1},\\\"a.b\\\":2}\","
            + " \"stringify_doc\": \"{\\\"n\\\":30,\\\"b\\\":true,\\\"x\\\":null}\","
            + " \"empty_doc\": \"{}\","
            + " \"malformed_doc\": \"{\\\"user\\\":{\\\"name\\\":\"}");
    client().performRequest(autoExtractDoc);

    // Auto-extract mode: null input handling (doc 1 establishes mapping, doc 2 has null)
    Request nullDoc1 = new Request("PUT", "/test_spath_null/_doc/1?refresh=true");
    nullDoc1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(nullDoc1);

    Request nullDoc2 = new Request("PUT", "/test_spath_null/_doc/2?refresh=true");
    nullDoc2.setJsonEntity("{\"doc\": null}");
    client().performRequest(nullDoc2);
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
    verifyDataRows(
        result,
        rows(new JSONObject("{\"n\":\"1\"}")),
        rows(new JSONObject("{\"n\":\"2\"}")),
        rows(new JSONObject("{\"n\":\"3\"}")));
  }

  @Test
  public void testSpathAutoExtractWithOutput() throws IOException {
    JSONObject result = executeQuery("source=test_spath | spath input=doc output=result");
    verifySchema(result, schema("doc", "string"), schema("result", "struct"));
    verifyDataRows(
        result,
        rows("{\"n\": 1}", new JSONObject("{\"n\":\"1\"}")),
        rows("{\"n\": 2}", new JSONObject("{\"n\":\"2\"}")),
        rows("{\"n\": 3}", new JSONObject("{\"n\":\"3\"}")));
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
    verifyDataRows(result, rows(new JSONObject("{\"tags{}\":\"[java, sql]\"}")));
  }

  @Test
  public void testSpathAutoExtractDuplicateKeysMerge() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=merge_doc output=result | fields result");

    // Duplicate logical keys merge into arrays: a.b from nested and dotted key
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"a.b\":\"[1, 2]\"}")));
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

  @Test
  public void testSpathAutoExtractNullInput() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath_null | spath input=doc output=result | fields result");

    // Non-null doc extracts normally, null doc returns null
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{\"n\":\"1\"}")), rows((Object) null));
  }

  @Test
  public void testSpathAutoExtractEmptyJson() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=empty_doc output=result | fields result");

    // Empty JSON object returns empty map
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{}")));
  }

  @Test
  public void testSpathAutoExtractMalformedJson() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=malformed_doc output=result | fields result");

    // Malformed JSON returns partial results parsed before the error
    verifySchema(result, schema("result", "struct"));
    verifyDataRows(result, rows(new JSONObject("{}")));
  }

  // Path navigation: access map values using dotted path syntax

  @Test
  public void testSpathWithEval() throws IOException {
    // result.user.name resolves to element_at(result, 'user.name')
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=nested_doc output=result"
                + " | eval name = result.user.name | fields name");
    verifySchema(result, schema("name", "string"));
    verifyDataRows(result, rows("John"));
  }

  @Test
  public void testSpathWithWhere() throws IOException {
    // Use eval to materialize map field before where to avoid AS-in-filter pushdown issue
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=nested_doc output=result"
                + " | eval name = result.user.name"
                + " | where name = 'John' | fields nested_doc");
    verifySchema(result, schema("nested_doc", "string"));
    verifyDataRows(result, rows("{\"user\":{\"name\":\"John\"}}"));
  }

  @Test
  public void testSpathWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath_auto | spath input=nested_doc output=result"
                + " | stats count() by result.user.name");
    verifySchema(result, schema("count()", "bigint"), schema("result.user.name", "string"));
    verifyDataRows(result, rows(1, "John"));
  }

  @Test
  public void testSpathWithSort() throws IOException {
    // spath path extraction + sort by extracted value
    JSONObject result =
        executeQuery(
            "source=test_spath | spath input=doc path=n"
                + " | sort - n | fields n");
    verifySchema(result, schema("n", "string"));
    verifyDataRowsInOrder(result, rows("3"), rows("2"), rows("1"));
  }
}
