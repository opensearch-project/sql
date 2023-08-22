/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class QueryIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void all_fields_test() throws IOException {
    String query = "SELECT * FROM " + TEST_INDEX_BEER + " WHERE query('*:taste')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void mandatory_params_test() throws IOException {
    String query = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('Tags:taste OR Body:taste')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query =
        "SELECT Id FROM "
            + TEST_INDEX_BEER
            + " WHERE query('Tags:taste', escape=false,allow_leading_wildcard=true,"
            + " enable_position_increments=true,fuzziness= 1, fuzzy_rewrite='constant_score',"
            + " max_determinized_states = 10000,analyzer='standard', analyze_wildcard = false,"
            + " quote_field_suffix = '.exact',auto_generate_synonyms_phrase_query=true, boost ="
            + " 0.77,quote_analyzer='standard', phrase_slop=0, rewrite='constant_score',"
            + " type='best_fields',tie_breaker=0.3, time_zone='Canada/Pacific',"
            + " default_operator='or',fuzzy_transpositions = false, lenient = true,"
            + " fuzzy_max_expansions = 25,minimum_should_match = '2<-25% 9<-3', fuzzy_prefix_length"
            + " = 7);";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void wildcard_test() throws IOException {
    String query1 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('Tags:taste')";
    JSONObject result1 = executeJdbcRequest(query1);
    String query2 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('*:taste')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query3 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('Tags:tas*');";
    JSONObject result3 = executeJdbcRequest(query3);
    assertEquals(8, result3.getInt("total"));

    String query4 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('Tags:tas?e');";
    JSONObject result4 = executeJdbcRequest(query3);
    assertEquals(8, result4.getInt("total"));
  }

  @Test
  public void query_string_and_query_return_the_same_results_test() throws IOException {
    String query1 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query('Tags:taste')";
    JSONObject result1 = executeJdbcRequest(query1);
    String query2 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE query_string(['Tags'],'taste')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertEquals(result2.getInt("total"), result1.getInt("total"));
  }
}
