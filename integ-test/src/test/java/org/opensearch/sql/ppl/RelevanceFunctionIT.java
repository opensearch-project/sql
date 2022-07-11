/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import org.junit.Test;

public class RelevanceFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void test_multi_match() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE multi_match([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste') | fields Id";
    var result = executeQuery(query);
    assertEquals(713, result.getInt("total"));
  }

  @Test
  public void test_simple_query_string() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste') | fields Id";
    var result = executeQuery(query);
    assertEquals(713, result.getInt("total"));
  }

  @Test
  public void test_multi_match_all_params() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE multi_match(['Body', Tags], 'taste beer', operator='and', analyzer=english,"
        + "auto_generate_synonyms_phrase_query=true, boost = 0.77, cutoff_frequency=0.33,"
        + "fuzziness = 'AUTO:1,5', fuzzy_transpositions = false, lenient = true, max_expansions = 25,"
        + "minimum_should_match = '2<-25% 9<-3', prefix_length = 7, tie_breaker = 0.3,"
        + "type = most_fields, slop = 2, zero_terms_query = 'ALL') | fields Id";
    var result = executeQuery(query);
    assertEquals(424, result.getInt("total"));
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
    assertEquals(1990, result.getInt("total"));
  }

  @Test
  public void test_wildcard_multi_match() throws IOException {
    String query1 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE multi_match(['Tags'], 'taste') | fields Id";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE multi_match(['T*'], 'taste') | fields Id";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));
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
  }
}
