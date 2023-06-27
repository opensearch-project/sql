/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.CONTENT_TYPE;

import java.io.IOException;
import java.util.Locale;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class SimpleQueryStringIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  /*
  The 'beer.stackexchange' index is a dump of beer.stackexchange.com converted to the format which might be ingested by OpenSearch.
  This is a forum like StackOverflow with questions about beer brewing. The dump contains both questions, answers and comments.
  The reference query is:
    select count(Id) from beer.stackexchange where simple_query_string(["Tags" ^ 1.5, Title, `Body` 4.2], 'taste') and Tags like '% % %' and Title like '%';
  It filters out empty `Tags` and `Title`.
  */

  @Test
  public void test_mandatory_params() throws IOException {
    String query = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void test_all_params() throws IOException {
    String query = "SELECT Id FROM " + TEST_INDEX_BEER
        + " WHERE simple_query_string(['Body', Tags, Title], 'taste beer', default_operator='or',"
        + "analyzer=english, analyze_wildcard = false, quote_field_suffix = '.exact',"
        + "auto_generate_synonyms_phrase_query=true, boost = 0.77, flags='PREFIX',"
        + "fuzzy_transpositions = false, lenient = true, fuzzy_max_expansions = 25,"
        + "minimum_should_match = '2<-25% 9<-3', fuzzy_prefix_length = 7);";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(49, result.getInt("total"));
  }

  @Test
  public void verify_wildcard_test() throws IOException {
    String query1 = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string(['Tags'], 'taste')";
    var result1 = new JSONObject(executeQuery(query1, "jdbc"));
    String query2 = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE simple_query_string(['T*'], 'taste')";
    var result2 = new JSONObject(executeQuery(query2, "jdbc"));
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query = "SELECT Id FROM " + TEST_INDEX_BEER
        + " WHERE simple_query_string(['*Date'], '2014-01-22');";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void contentHeaderTest() throws IOException {
    String query = "SELECT Id FROM " + TEST_INDEX_BEER
            + " WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, 'Body' 4.2], 'taste')";
    String requestBody = makeRequest(query);

    Request sqlRequest = new Request("POST", "/_plugins/_sql");
    sqlRequest.setJsonEntity(requestBody);

    Response response = client().performRequest(sqlRequest);

    assertEquals(response.getEntity().getContentType(), "content-type: " + CONTENT_TYPE);
  }
}
