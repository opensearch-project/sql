/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
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

  /** Bulk request body: 4 docs with nested JSON user object + 1 null doc. */
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
  public void testMapPathWithRename() throws IOException {
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
  public void testMapPathWithFillnull() throws IOException {
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
  public void testMapPathWithReplace() throws IOException {
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
  public void testMapPathWithFieldsExclusion() throws IOException {
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
  public void testMapPathWithAddtotals() throws IOException {
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
  public void testMapPathWithMvcombine() throws IOException {
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
        rows(new org.json.JSONArray("[\"John\"]"), "NYC"),
        rows(new org.json.JSONArray("[\"Alice\"]"), "LA"),
        rows(new org.json.JSONArray("[\"John\"]"), "SF"),
        rows(new org.json.JSONArray("[\"Bob\"]"), "NYC"),
        rows(null, null));
  }

  // ---- Edge cases ----

  /** Dotted field that resolves to a regular column (not MAP) should not cause errors. */
  @Test
  public void testNonMapDottedFieldSkippedGracefully() throws IOException {
    JSONObject result =
        ppl(
            """
            source=%s | spath input=doc
            | rename doc.user.name as username
            | fields - username
            | fields doc.user.age\
            """,
            TEST_INDEX);
    verifySchema(result, schema("doc.user.age", "string"));
    verifyDataRows(result, rows("30"), rows("25"), rows("35"), rows("40"), rows((Object) null));
  }

  /** Same MAP path referenced by two commands should not produce duplicate columns. */
  @Test
  public void testSameMapPathAcrossMultipleCommands() throws IOException {
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
