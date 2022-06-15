/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MultiMatchIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  /*
  The 'beer.stackexchange' index is a dump of beer.stackexchange.com converted to the format which might be ingested by OpenSearch.
  This is a forum like StackOverflow with questions about beer brewing. The dump contains both questions, answers and comments.
  The reference query is:
    select count(Id) from beer.stackexchange where multi_match(["Tags" ^ 1.5, Title, `Body` 4.2], 'taste') and Tags like '% % %' and Title like '%';
  It filters out empty `Tags` and `Title`.
  */

  @Test
  public void test1() throws IOException {
    String query = "SELECT count(*) FROM " + TEST_INDEX_BEER
        + " WHERE multi_match([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste')";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertNotEquals(0, result.getInt("total"));
  }

  @Test
  public void test_all_params() throws IOException {
    String query = "SELECT count(*) FROM " + TEST_INDEX_BEER
        + " WHERE multi_match(['Body', Tags], 'taste beer', operator='and', analyzer=english,"
        + "auto_generate_synonyms_phrase_query=true, boost = 0.77, cutoff_frequency=0.33,"
        + "fuzziness = 14, fuzzy_transpositions = false, lenient = true, max_expansions = 25,"
        + "minimum_should_match = '2<-25% 9<-3', prefix_length = 7, tie_breaker = 0.3,"
        + "type = most_fields, slop = 2, zero_terms_query = 'ALL');";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertNotEquals(0, result.getInt("total"));
  }

  @Test
  public void verify_wildcard_test() throws IOException {
    String query1 = "SELECT count(*) FROM " + TEST_INDEX_BEER
        + " WHERE multi_match(['Tags'], 'taste')";
    var result1 = new JSONObject(executeQuery(query1, "jdbc"));
    String query2 = "SELECT count(*) FROM " + TEST_INDEX_BEER
        + " WHERE multi_match(['T*'], 'taste')";
    var result2 = new JSONObject(executeQuery(query2, "jdbc"));
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));
  }
}
