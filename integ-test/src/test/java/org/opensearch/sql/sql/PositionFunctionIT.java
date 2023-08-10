/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class PositionFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.CALCS);
  }

  @Test
  public void position_function_test() {
    String query = "SELECT firstname, position('a' IN firstname) FROM %s";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_PEOPLE2));

    verifySchema(response, schema("firstname", null, "keyword"),
            schema("position('a' IN firstname)", null, "integer"));
    assertEquals(12, response.getInt("total"));

    verifyDataRows(response,
            rows("Daenerys", 2), rows("Hattie", 2),
            rows("Nanette", 2), rows("Dale", 2),
            rows("Elinor", 0), rows("Virginia", 8),
            rows("Dillard", 5), rows("Mcgee", 0),
            rows("Aurelia", 7), rows("Fulton", 0),
            rows("Burton", 0), rows("Josie", 0));
  }

  @Test
  public void position_function_with_nulls_test() {
    String query = "SELECT str2, position('ee' IN str2) FROM %s";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_CALCS));

    verifySchema(response, schema("str2", null, "keyword"),
            schema("position('ee' IN str2)", null, "integer"));
    assertEquals(17, response.getInt("total"));

    verifyDataRows(response,
            rows("one", 0), rows("two", 0),
            rows("three", 4), rows(null, null),
            rows("five", 0), rows("six", 0),
            rows(null, null), rows("eight", 0),
            rows("nine", 0), rows("ten", 0),
            rows("eleven", 0), rows("twelve", 0),
            rows(null, null), rows("fourteen", 6),
            rows("fifteen", 5), rows("sixteen", 5),
            rows(null, null));
  }

  @Test
  public void position_function_with_string_literals_test() {
    String query = "SELECT position('world' IN 'hello world')";
    JSONObject response = executeJdbcRequest(query);

    verifySchema(response, schema("position('world' IN 'hello world')", null, "integer"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(response, rows(7));
  }

  @Test
  public void position_function_with_only_fields_as_args_test() {
    String query = "SELECT position(str3 IN str2) FROM %s WHERE str2 IN ('one', 'two', 'three')";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_CALCS));

    verifySchema(response, schema("position(str3 IN str2)", null, "integer"));
    assertEquals(3, response.getInt("total"));

    verifyDataRows(response, rows(3), rows(0), rows(4));
  }

  @Test
  public void position_function_with_function_as_arg_test() {
    String query = "SELECT position(upper(str3) IN str1) FROM %s WHERE str1 LIKE 'BINDING SUPPLIES'";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_CALCS));

    verifySchema(response, schema("position(upper(str3) IN str1)", null, "integer"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(response, rows(15));
  }

  @Test
  public void position_function_in_where_clause_test() {
    String query = "SELECT str2 FROM %s WHERE position(str3 IN str2)=1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_CALCS));

    verifySchema(response, schema("str2", null, "keyword"));
    assertEquals(2, response.getInt("total"));

    verifyDataRows(response, rows("eight"), rows("eleven"));
  }

  @Test
  public void position_function_with_null_args_test() {
    String query1 = "SELECT str2, position(null IN str2) FROM %s WHERE str2 IN ('one')";
    String query2 = "SELECT str2, position(str2 IN null) FROM %s WHERE str2 IN ('one')";
    JSONObject response1 = executeJdbcRequest(String.format(query1, TestsConstants.TEST_INDEX_CALCS));
    JSONObject response2 = executeJdbcRequest(String.format(query2, TestsConstants.TEST_INDEX_CALCS));

    verifySchema(response1,
            schema("str2", null, "keyword"),
            schema("position(null IN str2)", null, "integer"));
    assertEquals(1, response1.getInt("total"));

    verifySchema(response2,
            schema("str2", null, "keyword"),
            schema("position(str2 IN null)", null, "integer"));
    assertEquals(1, response2.getInt("total"));

    verifyDataRows(response1, rows("one", null));
    verifyDataRows(response2, rows("one", null));
  }
}
