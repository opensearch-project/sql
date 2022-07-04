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

public class QueryStringIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void mandatory_params_test() throws IOException {
    String query = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE query_string([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(713, result.getInt("total"));
  }

  @Test
  public void all_params_test() throws IOException {
    String query = "SELECT Id FROM " + TEST_INDEX_BEER
        + " WHERE query_string(['Body', Tags, Title], 'taste beer',"
        + "allow_leading_wildcard=true, enable_position_increments=true,"
        + "fuzziness= 1, fuzzy_rewrite='constant_score', max_determinized_states = 10000,"
        + "analyzer='english', analyze_wildcard = false, quote_field_suffix = '.exact',"
        + "auto_generate_synonyms_phrase_query=true, boost = 0.77,"
        + "quote_analyzer='standard', phrase_slop=0, rewrite='constant_score', type='best_fields',"
        + "tie_breaker=0.3, time_zone='Canada/Pacific', default_operator='or',"
        + "fuzzy_transpositions = false, lenient = true, fuzzy_max_expansions = 25,"
        + "minimum_should_match = '2<-25% 9<-3', fuzzy_prefix_length = 7);";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(1990, result.getInt("total"));
  }

  @Test
  public void wildcard_test() throws IOException {
    String query1 = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE query_string(['Tags'], 'taste')";
    var result1 = new JSONObject(executeQuery(query1, "jdbc"));
    String query2 = "SELECT Id FROM "
        + TEST_INDEX_BEER + " WHERE query_string(['T*'], 'taste')";
    var result2 = new JSONObject(executeQuery(query2, "jdbc"));
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query3 = "SELECT Id FROM " + TEST_INDEX_BEER
        + " WHERE query_string(['*Date'], '2015-01-29');";
    var result3 = new JSONObject(executeQuery(query3, "jdbc"));
    assertEquals(5, result3.getInt("total"));
  }
}
