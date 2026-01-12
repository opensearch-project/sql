/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.array;
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
    putItem(4, "join1", sj("{'key': 'k1', 'left': 'l'}"));
    putItem(5, "join2", sj("{'key': 'k1', 'right': 'r1'}"));
    putItem(6, "join2", sj("{'key': 'k2', 'right': 'r2'}"));
    putItem(7, "overwrap", sj("{'a.b': 1, 'a': {'b': 2, 'c': 3}}"));
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

  private static final String EXPECTED_ARBITRARY_FIELD_ERROR =
      "Spath command cannot extract arbitrary fields. "
          + "Please project fields explicitly by fields command without wildcard or stats command.";

  @Test
  public void testSpathWithoutFields() throws IOException {
    verifyExplainException(
        "source=test_spath | spath input=doc | eval a = 1", EXPECTED_ARBITRARY_FIELD_ERROR);
  }

  @Test
  public void testSpathWithWildcard() throws IOException {
    verifyExplainException(
        "source=test_spath | spath input=doc | fields a, b*", EXPECTED_ARBITRARY_FIELD_ERROR);
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
    verifySchema(result, schema("a", "array"), schema("doc", "string"));
    verifyDataRows(result, rows(array("1", "1"), sj("{'a': 1, 'b': 2, 'c': 3}")));
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
}
