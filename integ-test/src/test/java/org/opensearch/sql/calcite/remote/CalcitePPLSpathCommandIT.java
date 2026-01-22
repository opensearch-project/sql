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
  private static final String INDEX = "test_spath";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    putItem(1, "simple", sj("{'a': 1, 'b': 2, 'c': 3}"));
    putItem(2, "simple", sj("{'a': 1, 'b': 2, 'c': 3}"));
    putItem(3, "nested", sj("{'nested': {'d': [1, 2, 3], 'e': 'str'}}"));
    putItem(4, "join1", sj("{'key': 'k1', 'left': 'l', 'common': 'cLeft'}"));
    putItem(5, "join2", sj("{'key': 'k1', 'right': 'r1', 'common': 'cRight'}"));
    putItem(6, "join2", sj("{'key': 'k2', 'right': 'r2', 'common': 'cRight'}"));
    putItem(7, "overwrap", sj("{'a.b': 1, 'a': {'b': 2, 'c': 3}}"));
    putItem(8, "types", sj("{'string': 'STRING', 'boolean': true, 'number': 10.1, 'null': null}"));
  }

  private void putItem(int id, String testCase, String json) throws Exception {
    Request request = new Request("PUT", String.format("/%s/_doc/%d?refresh=true", INDEX, id));
    request.setJsonEntity(docWithJson(testCase, json));
    client().performRequest(request);
  }

  private String docWithJson(String testCase, String json) {
    return String.format(sj("{'testCase': '%s', 'doc': '%s'}"), testCase, escape(json));
  }

  private String escape(String json) {
    return json.replace("\"", "\\\"");
  }

  private String sj(String singleQuoteJson) {
    return singleQuoteJson.replace("'", "\"");
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc output=result path=a |"
                + " fields result | head 2");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("1"));
  }

  private static final String EXPECTED_SPATH_WILDCARD_ERROR =
      "Spath command cannot be used with partial wildcard such as `prefix*`.";

  @Test
  public void testSpathWithWildcard() throws IOException {
    verifyExplainException(
        "source=test_spath | spath input=doc | fields a, b*", EXPECTED_SPATH_WILDCARD_ERROR);
  }

  @Test
  public void testSpathWithoutFields() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | where testCase='simple' | spath input=doc | head 1");
    verifySchema(
        result,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("doc", "string"),
        schema("testCase", "string"));
    verifyDataRows(result, rows("1", "2", "3", sj("{'a': 1, 'b': 2, 'c': 3}"), "simple"));
  }

  @Test
  public void testSpathWithOnlyWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | fields * | head 1");
    verifySchema(
        result,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("doc", "string"),
        schema("testCase", "string"));
    verifyDataRows(result, rows("1", "2", "3", sj("{'a': 1, 'b': 2, 'c': 3}"), "simple"));
  }

  @Test
  public void testSpathWithFieldAndWildcard() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | fields c, * | head 1");
    verifySchema(
        result,
        schema("c", "string"),
        schema("a", "string"),
        schema("b", "string"),
        schema("doc", "string"),
        schema("testCase", "string"));
    verifyDataRows(result, rows("3", "1", "2", sj("{'a': 1, 'b': 2, 'c': 3}"), "simple"));
  }

  @Test
  public void testSpathWithFieldAndWildcardAtMiddle() throws IOException {
    verifyExplainException(
        "source=test_spath | where testCase='simple' | spath input=doc | fields c, *, b",
        "Wildcard can be placed only at the end of the fields list (limit of spath command).");
  }

  @Test
  public void testSpathTypes() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | where testCase='types' | spath input=doc | head 1");
    verifySchema(
        result,
        schema("boolean", "string"),
        schema("doc", "string"),
        schema("null", "string"),
        schema("number", "string"),
        schema("string", "string"),
        schema("testCase", "string"));
    verifyDataRows(
        result,
        rows(
            "true",
            sj("{'string': 'STRING', 'boolean': true, 'number': 10.1, 'null': null}"),
            null,
            "10.1",
            "STRING",
            "types"));
  }

  private static final String EXPECTED_SUBQUERY_ERROR =
      "Filter by subquery is not supported with field resolution.";

  @Test
  public void testSpathWithSubsearch() throws IOException {
    verifyExplainException(
        "source=test_spath | spath input=doc | where b in [source=test_spath | fields a] | fields"
            + " b",
        EXPECTED_SUBQUERY_ERROR);
  }

  @Test
  public void testSpathWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | fields a, b, c | head"
                + " 1");
    verifySchema(result, schema("a", "string"), schema("b", "string"), schema("c", "string"));
    verifyDataRows(result, rows("1", "2", "3"));
  }

  @Test
  public void testSpathWithAbsentField() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | fields a, x | head 1");
    verifySchema(result, schema("a", "string"), schema("x", "string"));
    verifyDataRows(result, rows("1", null));
  }

  @Test
  public void testOverwrap() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='overwrap' | spath input=doc | fields a.b | head"
                + " 1");
    verifySchema(result, schema("a.b", "string"));
    verifyDataRows(result, rows("[1, 2]"));
  }

  @Test
  public void testSpathTwice() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | spath input=doc |"
                + " fields a, doc | head 1");
    verifySchema(result, schema("a", "string"), schema("doc", "string"));
    verifyDataRows(result, rows("[1, 1]", sj("{'a': 1, 'b': 2, 'c': 3}")));
  }

  @Test
  public void testSpathTwiceWithDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc | spath input=doc |"
                + " fields b, * | head 1");
    verifySchema(
        result,
        schema("b", "string"),
        schema("a", "string"),
        schema("c", "string"),
        schema("doc", "string"),
        schema("testCase", "string"));
    verifyDataRows(
        result, rows("[2, 2]", "[1, 1]", "[3, 3]", sj("{'a': 1, 'b': 2, 'c': 3}"), "simple"));
  }

  @Test
  public void testSpathWithEval() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc |"
                + " eval result = a * b * c | fields result | head 1");
    verifySchema(result, schema("result", "double"));
    verifyDataRows(result, rows(6));
  }

  @Test
  public void testSpathWithStats() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='simple' | spath input=doc |"
                + "stats count by a, b | head 1");
    verifySchema(result, schema("count", "bigint"), schema("a", "string"), schema("b", "string"));
    verifyDataRows(result, rows(2, "1", "2"));
  }

  @Test
  public void testSpathWithNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='nested' | spath input=doc | fields `nested.d{}`,"
                + " nested.e");
    verifySchema(result, schema("nested.d{}", "string"), schema("nested.e", "string"));
    verifyDataRows(result, rows("[1, 2, 3]", "str"));
  }

  @Test
  public void testSpathWithJoin() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='join1' | spath input=doc | fields key, left | join"
                + " key [source=test_spath | where testCase='join2' | spath input=doc | fields key,"
                + " right ] |fields key, left, right");
    verifySchema(
        result, schema("key", "string"), schema("left", "string"), schema("right", "string"));
    verifyDataRows(result, rows("k1", "l", "r1"));
  }

  @Test
  public void testSpathWithJoinWithFieldsAndDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='join1' | spath input=doc | "
                + "join key [source=test_spath | where testCase='join2' | spath input=doc ]");
    verifySchema(
        result,
        schema("key", "string"),
        schema("common", "string"),
        schema("doc", "string"),
        schema("left", "string"),
        schema("right", "string"),
        schema("testCase", "string"));
    verifyDataRows(
        result,
        rows(
            "k1",
            "cRight",
            sj("{'key': 'k1', 'right': 'r1', 'common': 'cRight'}"),
            "l",
            "r1",
            "join2"));
  }

  @Test
  public void testSpathWithJoinOverwriteWithFieldsAndDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='join1' | spath input=doc | join overwrite=false"
                + " key [source=test_spath | where testCase='join2' | spath input=doc ]");
    verifySchema(
        result,
        schema("key", "string"),
        schema("common", "string"),
        schema("doc", "string"),
        schema("left", "string"),
        schema("right", "string"),
        schema("testCase", "string"));
    verifyDataRows(
        result,
        rows(
            "k1",
            "cLeft",
            sj("{'key': 'k1', 'left': 'l', 'common': 'cLeft'}"),
            "l",
            "r1",
            "join1"));
  }

  @Test
  public void testSpathWithJoinWithCriteriaAndDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_spath | where testCase='join1' | spath input=doc | join left=l right=r on"
                + " l.key = r.key [source=test_spath | where testCase='join2' | spath input=doc ]");
    verifySchema(
        result,
        schema("key", "string"),
        schema("r.key", "string"),
        schema("common", "string"),
        schema("doc", "string"),
        schema("left", "string"),
        schema("right", "string"),
        schema("testCase", "string"));
    verifyDataRows(
        result,
        rows(
            "k1",
            "k1",
            "cLeft",
            sj("{'key': 'k1', 'left': 'l', 'common': 'cLeft'}"),
            "l",
            "r1",
            "join1"));
  }
}
