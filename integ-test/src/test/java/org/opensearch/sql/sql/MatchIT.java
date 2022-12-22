/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.utils.StringUtils;

public class MatchIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void match_in_where() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_ACCOUNT + " WHERE match(lastname, 'Bates')");
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Nanette"));
  }

  @Test
  public void match_in_having() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT lastname FROM " + TEST_INDEX_ACCOUNT + " HAVING match(firstname, 'Nanette')");
    verifySchema(result, schema("lastname", "text"));
    verifyDataRows(result, rows("Bates"));
  }

  @Test
  public void missing_field_test() {
    String query = StringUtils.format("SELECT * FROM %s WHERE match(invalid, 'Bates')", TEST_INDEX_ACCOUNT);
    final RuntimeException exception =
        expectThrows(RuntimeException.class, () -> executeJdbcRequest(query));

    assertTrue(exception.getMessage()
        .contains("can't resolve Symbol(namespace=FIELD_NAME, name=invalid) in type env"));

    assertTrue(exception.getMessage().contains("SemanticCheckException"));
  }

  @Test
  public void missing_quoted_field_test() {
    String query = StringUtils.format("SELECT * FROM %s WHERE match('invalid', 'Bates')", TEST_INDEX_ACCOUNT);
    final RuntimeException exception =
        expectThrows(RuntimeException.class, () -> executeJdbcRequest(query));

    assertTrue(exception.getMessage()
        .contains("can't resolve Symbol(namespace=FIELD_NAME, name=invalid) in type env"));

    assertTrue(exception.getMessage().contains("SemanticCheckException"));
  }


  @Test
  public void matchquery_in_where() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_ACCOUNT + " WHERE matchquery(lastname, 'Bates')");
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Nanette"));
  }

  @Test
  public void matchquery_in_having() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT lastname FROM " + TEST_INDEX_ACCOUNT + " HAVING matchquery(firstname, 'Nanette')");
    verifySchema(result, schema("lastname", "text"));
    verifyDataRows(result, rows("Bates"));
  }

  @Test
  public void match_query_in_where() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_ACCOUNT + " WHERE match_query(lastname, 'Bates')");
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Nanette"));
  }

  @Test
  public void match_query_in_having() throws IOException {
    JSONObject result = executeJdbcRequest(
        "SELECT lastname FROM " + TEST_INDEX_ACCOUNT + " HAVING match_query(firstname, 'Nanette')");
    verifySchema(result, schema("lastname", "text"));
    verifyDataRows(result, rows("Bates"));
  }

  @Test
  public void alternate_syntaxes_return_the_same_results() throws IOException {
    String query1 = "SELECT lastname FROM "
        + TEST_INDEX_ACCOUNT + " HAVING match(firstname, 'Nanette')";
    JSONObject result1 = executeJdbcRequest(query1);
    String query2 = "SELECT lastname FROM "
        + TEST_INDEX_ACCOUNT + " HAVING matchquery(firstname, 'Nanette')";
    JSONObject result2 = executeJdbcRequest(query2);
    String query3 = "SELECT lastname FROM "
        + TEST_INDEX_ACCOUNT + " HAVING match_query(firstname, 'Nanette')";
    JSONObject result3 = executeJdbcRequest(query3);
    assertEquals(result1.getInt("total"), result2.getInt("total"));
    assertEquals(result1.getInt("total"), result3.getInt("total"));
  }
}
