/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for dynamic wildcard functionality in fields command. Tests wildcard patterns
 * that match dynamic columns stored in _dynamic_columns MAP field.
 */
public class CalcitePPLDynamicWildcardIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "test_dynamic_wildcard";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestData();
  }

  private void createTestData() throws IOException {
    createDocumentWithIdAndJsonField(
        TEST_INDEX,
        1,
        "details",
        "{\"user_id\": \"123\", \"user_name\": \"john\", \"session_token\": \"abc\","
            + " \"error_code\": \"404\"}");
  }

  @Test
  public void testPositiveDynamicWildcard() throws IOException {
    JSONObject result = executeQuery(source(TEST_INDEX, "spath input=details | fields user*"));

    verifyColumn(result, columnName("user_id"), columnName("user_name"));
    verifySchema(result, schema("user_id", "string"), schema("user_name", "string"));
  }

  @Test
  public void testMultiplePositiveDynamicWildcards() throws IOException {
    JSONObject result =
        executeQuery(source(TEST_INDEX, "spath input=details | fields user*, *_token"));

    verifyColumn(
        result, columnName("user_id"), columnName("user_name"), columnName("session_token"));
    verifySchema(
        result,
        schema("user_id", "string"),
        schema("user_name", "string"),
        schema("session_token", "string"));
  }

  @Test
  public void testNegativeDynamicWildcard() throws IOException {
    JSONObject result =
        executeQuery(source(TEST_INDEX, "spath input=details | fields - user*, *_code"));

    verifyColumn(result, columnName("details"), columnName("id"), columnName("session_token"));
    verifySchema(
        result,
        schema("details", "string"),
        schema("id", "bigint"),
        schema("session_token", "string"));
  }

  @Test
  public void testDynamicWildcardWithStaticFields() throws IOException {
    JSONObject result = executeQuery(source(TEST_INDEX, "spath input=details | fields id, user*"));

    verifyColumn(result, columnName("id"), columnName("user_id"), columnName("user_name"));
    verifySchema(
        result, schema("id", "bigint"), schema("user_id", "string"), schema("user_name", "string"));
  }

  @Test
  public void testNoMatchingDynamicWildcard() throws IOException {
    JSONObject result =
        executeQuery(source(TEST_INDEX, "spath input=details | fields nonexistent*"));

    verifyColumn(result);
  }
}
