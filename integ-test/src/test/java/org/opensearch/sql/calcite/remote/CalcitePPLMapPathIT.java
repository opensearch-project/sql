/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for PPL queries that reference MAP dotted paths (e.g. {@code doc.user.name}).
 * Uses {@code spath} to parse a JSON text field into a MAP column, then verifies various PPL
 * commands work correctly on the resulting nested paths.
 */
public class CalcitePPLMapPathIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "opensearch-sql_test_index_spath";

  private static final String TEST_BULK_DATA =
      """
      {"index":{"_id":"1"}}
      {"doc":"{\\"user\\": {\\"name\\": \\"John\\",  \\"age\\": 30, \\"city\\": \\"NYC\\"}}"}
      {"index":{"_id":"2"}}
      {"doc":"{\\"user\\": {\\"name\\": \\"Alice\\", \\"age\\": 25, \\"city\\": \\"LA\\"}}"}
      {"index":{"_id":"3"}}
      {"doc":"{\\"user\\": {\\"name\\": \\"John\\",  \\"age\\": 35, \\"city\\": \\"SF\\"}}"}
      {"index":{"_id":"4"}}
      {"doc":"{\\"user\\": {\\"name\\": \\"Bob\\",   \\"age\\": 40, \\"city\\": \\"NYC\\"}}"}
      {"index":{"_id":"5"}}
      {"doc":null}
      """;

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createJsonTestIndex();
  }

  @Test
  public void testRenameOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | rename doc.user.name as username
            | fields username, doc.user.age\
            """,
            TEST_INDEX);
    verifySchema(result, schema("username", "string"), schema("doc.user.age", "string"));
    verifyDataRows(
        result,
        rows("John", "30"),
        rows("Alice", "25"),
        rows("John", "35"),
        rows("Bob", "40"),
        rows(null, null));
  }

  @Test
  public void testFillnullOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | fillnull using doc.user.name = 'N/A'
            | fields doc.user.name\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"), rows("Alice"), rows("John"), rows("Bob"), rows("N/A"));
  }

  @Test
  public void testReplaceOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | replace 'John' WITH 'Jonathan' IN doc.user.name
            | fields doc.user.name\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(
        result,
        rows("Jonathan"),
        rows("Alice"),
        rows("Jonathan"),
        rows("Bob"),
        rows((Object) null));
  }

  @Test
  public void testFieldsExclusionOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | fields - doc.user.name
            | fields doc.user.age, doc.user.city\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.age", "string"), schema("doc.user.city", "string"));
    verifyDataRows(
        result,
        rows("30", "NYC"),
        rows("25", "LA"),
        rows("35", "SF"),
        rows("40", "NYC"),
        rows(null, null));
  }

  @Test
  public void testAddtotalsOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | eval age_num = cast(doc.user.age as double)
            | addtotals col=true row=false age_num
            | fields doc.user.name, age_num\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"), schema("age_num", "double"));
    verifyDataRows(
        result,
        rows("John", 30.0),
        rows("Alice", 25.0),
        rows("John", 35.0),
        rows("Bob", 40.0),
        rows(null, null),
        rows(null, 130.0));
  }

  @Test
  public void testMvcombineOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | mvcombine doc.user.name
            | fields doc.user.name, doc.user.city\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "array"), schema("doc.user.city", "string"));
    verifyDataRows(
        result,
        rows(new JSONArray("[\"John\"]"), "NYC"),
        rows(new JSONArray("[\"Alice\"]"), "LA"),
        rows(new JSONArray("[\"John\"]"), "SF"),
        rows(new JSONArray("[\"Bob\"]"), "NYC"),
        rows(null, null));
  }

  @Test
  public void testRenameWildcardOnMapPath() throws IOException {
    verifySchema(
        ppl(
            """
            source=%s | spath input=doc
            | rename d* as test_*
            | fields test_oc\
            """,
            TEST_INDEX),
        schema("test_oc", "struct"));
  }

  @Test
  public void testFieldsExclusionWildcardOnMapPath() throws IOException {
    verifySchema(
        ppl(
            """
            source=%s | spath input=doc
            | fields - doc.user.na*
            | fields doc.user.age, doc.user.city\
            """,
            TEST_INDEX),
        schema("doc.user.age", "string"),
        schema("doc.user.city", "string"));
  }

  @Test
  public void testMultipleCommandsOnSameMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | fillnull using doc.user.name = 'N/A'
            | rename doc.user.name as username
            | fields username\
            """,
            TEST_INDEX);
    verifySchema(result, schema("username", "string"));
    verifyDataRows(result, rows("John"), rows("Alice"), rows("John"), rows("Bob"), rows("N/A"));
  }

  @Test
  public void testWhereOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | where doc.user.name = 'John'
            | fields doc.user.name, doc.user.city\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"), schema("doc.user.city", "string"));
    verifyDataRows(result, rows("John", "NYC"), rows("John", "SF"));
  }

  @Test
  public void testEvalOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | eval name = doc.user.name
            | fields name\
            """,
            TEST_INDEX);
    verifySchema(result, schema("name", "string"));
    verifyDataRows(
        result, rows("John"), rows("Alice"), rows("John"), rows("Bob"), rows((Object) null));
  }

  @Test
  public void testStatsOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | stats count() as cnt by doc.user.city\
            """,
            TEST_INDEX);
    verifySchema(result, schema("cnt", "bigint"), schema("doc.user.city", "string"));
    verifyDataRows(result, rows(1, null), rows(1, "LA"), rows(2, "NYC"), rows(1, "SF"));
  }

  @Test
  public void testSortOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | sort doc.user.name
            | fields doc.user.name\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRowsInOrder(
        result, rows((Object) null), rows("Alice"), rows("Bob"), rows("John"), rows("John"));
  }

  @Test
  public void testDedupOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | dedup 1 doc.user.name
            | fields doc.user.name\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"), rows("Alice"), rows("Bob"));
  }

  @Test
  public void testEventstatsOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | eventstats count() as cnt by doc.user.city
            | where doc.user.city = 'NYC'
            | fields doc.user.name, cnt\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"), schema("cnt", "bigint"));
    verifyDataRows(result, rows("John", 2), rows("Bob", 2));
  }

  @Test
  public void testChartOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | chart count() by doc.user.city\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.city", "string"), schema("count()", "bigint"));
    verifyDataRows(result, rows("LA", 1), rows("NYC", 2), rows("SF", 1));
  }

  @Test
  public void testTrendlineOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | eval age_num = cast(doc.user.age as double)
            | where isnotnull(age_num)
            | trendline sma(2, age_num) as trend
            | fields doc.user.name, trend\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.name", "string"), schema("trend", "double"));
    verifyNumOfRows(result, 4);
  }

  @Test
  public void testParseOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | parse doc.user.city '(?<firstchar>.).*'
            | fields doc.user.city, firstchar\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.city", "string"), schema("firstchar", "string"));
    verifyDataRows(
        result,
        rows("NYC", "N"),
        rows("LA", "L"),
        rows("SF", "S"),
        rows("NYC", "N"),
        rows(null, ""));
  }

  @Test
  public void testRexOnMapPath() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | rex field=doc.user.city \\\"(?<firstchar>.).*\\\""
                    + " | fields doc.user.city, firstchar",
                TEST_INDEX));
    verifySchema(result, schema("doc.user.city", "string"), schema("firstchar", "string"));
    verifyDataRows(
        result,
        rows("NYC", "N"),
        rows("LA", "L"),
        rows("SF", "S"),
        rows("NYC", "N"),
        rows(null, null));
  }

  @Test
  public void testPatternsOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | patterns doc.user.city
            | head 1
            | fields doc.user.city, patterns_field\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.city", "string"), schema("patterns_field", "string"));
    verifyNumOfRows(result, 1);
  }

  @Test
  public void testBinOnMapPath() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | eval age_num = cast(doc.user.age as integer)
            | where isnotnull(age_num)
            | stats count() as cnt by span(age_num, 10) as age_bin
            | fields age_bin, cnt\
            """,
            TEST_INDEX);
    verifySchema(result, schema("age_bin", "int"), schema("cnt", "bigint"));
    verifyDataRows(result, rows(20, 1), rows(30, 2), rows(40, 1));
  }

  private void createJsonTestIndex() {
    if (isIndexExist(client(), TEST_INDEX)) {
      return;
    }
    createIndexByRestClient(client(), TEST_INDEX, null);
    Request request = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
    request.setJsonEntity(TEST_BULK_DATA);
    performRequest(client(), request);
  }

  private JSONObject ppl(String query, Object... args) throws IOException {
    return executeQuery(String.format(query, args).replace('\n', ' '));
  }
}
