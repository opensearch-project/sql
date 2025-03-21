/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class MultiMatchIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BEER);
  }

  @Test
  public void test_multi_match() throws IOException {
    String query =
        "SOURCE="
            + TEST_INDEX_BEER
            + " | WHERE multi_match([\\\"Tags\\\" ^ 1.5, Title, 'Body' 4.2], 'taste') | fields Id";
    var result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void test_multi_match_all_params() throws IOException {
    String query =
        "SOURCE="
            + TEST_INDEX_BEER
            + " | WHERE multi_match(['Body', Tags], 'taste beer', operator='and',"
            + " analyzer=english,auto_generate_synonyms_phrase_query=true, boost = 0.77,"
            + " cutoff_frequency=0.33,fuzziness = 'AUTO:1,5', fuzzy_transpositions = false, lenient"
            + " = true, max_expansions = 25,minimum_should_match = '2<-25% 9<-3', prefix_length ="
            + " 7, tie_breaker = 0.3,type = most_fields, slop = 2, zero_terms_query = 'ALL') |"
            + " fields Id";
    var result = executeQuery(query);
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void test_wildcard_multi_match() throws IOException {
    String query1 =
        "SOURCE=" + TEST_INDEX_BEER + " | WHERE multi_match(['Tags'], 'taste') | fields Id";
    var result1 = executeQuery(query1);
    String query2 =
        "SOURCE=" + TEST_INDEX_BEER + " | WHERE multi_match(['T*'], 'taste') | fields Id";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query3 =
        "source=" + TEST_INDEX_BEER + " | where simple_query_string(['*Date'], '2014-01-22')";
    JSONObject result3 = executeQuery(query3);
    assertEquals(10, result3.getInt("total"));
  }
}
