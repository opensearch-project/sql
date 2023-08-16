/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class StringLiteralIT extends SQLIntegTestCase {
  @Test
  public void testStringHelloSingleQuote() throws IOException {
    JSONObject result =
        executeJdbcRequest("select 'Hello'");
    verifySchema(result,
        schema("'Hello'", null, "keyword"));
    verifyDataRows(result, rows("Hello"));
  }

  @Test
  public void testStringHelloDoubleQuote() throws IOException {
    JSONObject result =
        executeJdbcRequest("select \\\"Hello\\\"");
    verifySchema(result,
        schema("\"Hello\"", null, "keyword"));
    verifyDataRows(result, rows("Hello"));
  }

  @Test
  public void testImStringDoubleDoubleQuoteEscape() throws IOException {
    JSONObject result =
        executeJdbcRequest("select \\\"I\\\"\\\"m\\\"");
    verifySchema(result,
        schema("\"I\"\"m\"", null, "keyword"));
    verifyDataRows(result, rows("I\"m"));
  }

  @Test
  public void testImStringDoubleSingleQuoteEscape() throws IOException {
    JSONObject result =
        executeJdbcRequest("select 'I''m'");
    verifySchema(result,
        schema("'I''m'", null, "keyword"));
    verifyDataRows(result, rows("I'm"));
  }

  @Test
  public void testImStringEscapedSingleQuote() throws IOException {
    JSONObject result =
        executeJdbcRequest("select 'I\\\\'m'");
    verifySchema(result,
        schema("'I\\'m'", null, "keyword"));
    verifyDataRows(result, rows("I'm"));
  }
}
