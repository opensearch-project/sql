/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class SimpleQueryStringIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void test_simple_query_string() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, 'Body' 4.2], 'taste') | fields Id";
    var result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void test_simple_query_string_all_params() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['Body', Tags, Title], 'taste beer', default_operator='or',"
        + "analyzer=english, analyze_wildcard = false, quote_field_suffix = '.exact',"
        + "auto_generate_synonyms_phrase_query=true, boost = 0.77, flags='PREFIX',"
        + "fuzzy_transpositions = false, lenient = true, fuzzy_max_expansions = 25,"
        + "minimum_should_match = '2<-25% 9<-3', fuzzy_prefix_length = 7) | fields Id";
    var result = executeQuery(query);
    assertEquals(49, result.getInt("total"));
  }

  @Test
  public void test_wildcard_simple_query_string() throws IOException {
    String query1 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['Tags'], 'taste') | fields Id";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['T*'], 'taste') | fields Id";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query3 = "source=" + TEST_INDEX_BEER
        + " | where simple_query_string(['*Date'], '2014-01-22')";
    JSONObject result3 = executeQuery(query3);
    assertEquals(10, result3.getInt("total"));
  }
}
