/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MatchBoolPrefixIT extends SQLIntegTestCase {
  public void init() throws IOException {
    loadIndex(SQLIntegTestCase.Index.PHRASE);
  }

  @Test
  public void query_matches_test() throws IOException {
    String query = "SELECT phrase FROM "
        + TEST_INDEX_PHRASE + " WHERE match_bool_prefix(phrase, 'quick')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    verifySchema(result, schema("phrase", "text"));

    verifyDataRows(result,
        rows("quick fox"),
        rows("quick fox here"));
  }

  @Test
  public void additional_parameters_test() throws IOException {
    String query = "SELECT phrase FROM "
        + TEST_INDEX_PHRASE + " WHERE match_bool_prefix(phrase, '2 test', minimum_should_match=1, fuzziness=2)";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    verifySchema(result, schema("phrase", "text"));

    verifyDataRows(result,
        rows("my test"),
        rows("my test 2"));
  }

  @Test
  public void no_matches_test() throws IOException {
    String query = "SELECT * FROM "
        + TEST_INDEX_PHRASE + " WHERE match_bool_prefix(phrase, 'rice')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(0, result.getInt("total"));
  }
}
