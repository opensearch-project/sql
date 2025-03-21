/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class QueryStringIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BEER);
  }

  @Test
  public void all_fields_test() throws IOException {
    String query = "source=" + TEST_INDEX_BEER + " | where query_string(['*'], 'taste')";
    JSONObject result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void mandatory_params_test() throws IOException {
    String query =
        "source="
            + TEST_INDEX_BEER
            + " | where query_string([\\\"Tags\\\" ^ 1.5, Title, 'Body' 4.2], 'taste')";
    JSONObject result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query =
        "source="
            + TEST_INDEX_BEER
            + " | where query_string(['Body', Tags, Title], 'taste"
            + " beer',allow_leading_wildcard=true, enable_position_increments=true,"
            + " escape=false,fuzziness= 1, fuzzy_rewrite='constant_score', max_determinized_states"
            + " = 10000,analyzer='english', analyze_wildcard = false, quote_field_suffix ="
            + " '.exact',auto_generate_synonyms_phrase_query=true, boost ="
            + " 0.77,quote_analyzer='standard', phrase_slop=0, rewrite='constant_score',"
            + " type='best_fields',tie_breaker=0.3, time_zone='Canada/Pacific',"
            + " default_operator='or',fuzzy_transpositions = false, lenient = true,"
            + " fuzzy_max_expansions = 25,minimum_should_match = '2<-25% 9<-3', fuzzy_prefix_length"
            + " = 7)";
    JSONObject result = executeQuery(query);
    assertEquals(49, result.getInt("total"));
  }

  @Test
  public void wildcard_test() throws IOException {

    String query1 = "source=" + TEST_INDEX_BEER + " | where query_string(['Tags'], 'taste')";
    JSONObject result1 = executeQuery(query1);

    String query2 = "source=" + TEST_INDEX_BEER + " | where query_string(['T*'], 'taste')";
    JSONObject result2 = executeQuery(query2);
    assertNotEquals(result1.getInt("total"), result2.getInt("total"));

    String query3 = "source=" + TEST_INDEX_BEER + " | where query_string(['*Date'], '2014-01-22')";
    JSONObject result3 = executeQuery(query3);
    assertEquals(10, result3.getInt("total"));
  }
}
