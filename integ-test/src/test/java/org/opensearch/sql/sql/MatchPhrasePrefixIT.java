/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MatchPhrasePrefixIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void required_parameters() throws IOException {
    String query = "SELECT Title FROM %s WHERE match_phrase_prefix(Title, 'champagne be')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(
        result,
        rows("Can old flat champagne be used for vinegar?"),
        rows("Elder flower champagne best to use natural yeast or add a wine yeast?"));
  }

  @Test
  public void all_optional_parameters() throws IOException {
    // The values for optional parameters are valid but arbitrary.
    String query =
        "SELECT Title FROM %s "
            + "WHERE match_phrase_prefix(Title, 'flat champ', boost = 1.0, zero_terms_query='ALL', "
            + "max_expansions = 2, analyzer=standard, slop=0)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("Can old flat champagne be used for vinegar?"));
  }

  @Test
  public void max_expansions_is_3() throws IOException {
    // max_expansions applies to the last term in the query -- 'bottl'
    // It tells OpenSearch to consider only the first 3 terms that start with 'bottl'
    // In this dataset these are 'bottle-conditioning', 'bottling', 'bottles'.

    String query =
        "SELECT Tags FROM %s "
            + "WHERE match_phrase_prefix(Tags, 'draught bottl', max_expansions=3)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("brewing draught bottling"), rows("draught bottles"));
  }

  @Test
  public void analyzer_english() throws IOException {
    // English analyzer removes 'in' and 'to' as they are common words.
    // This results in an empty query.
    String query =
        "SELECT Title FROM %s " + "WHERE match_phrase_prefix(Title, 'in to', analyzer=english)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    assertTrue(
        "Expect English analyzer to filter out common words 'in' and 'to'",
        result.getInt("total") == 0);
  }

  @Test
  public void analyzer_standard() throws IOException {
    // Standard analyzer does not treat 'in' and 'to' as special terms.
    // This results in 'to' being used as a phrase prefix given us 'Tokyo'.
    String query =
        "SELECT Title FROM %s " + "WHERE match_phrase_prefix(Title, 'in to', analyzer=standard)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("Local microbreweries and craft beer in Tokyo"));
  }

  @Test
  public void zero_term_query_all() throws IOException {
    // English analyzer removes 'in' and 'to' as they are common words.
    // zero_terms_query of 'ALL' causes all rows to be returned.
    // ORDER BY ... LIMIT helps make the test understandable.
    String query =
        "SELECT Title FROM %s"
            + " WHERE match_phrase_prefix(Title, 'in to', analyzer=english, zero_terms_query='ALL')"
            + " ORDER BY Title DESC"
            + " LIMIT 1";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("was working great, now all foam"));
  }

  @Test
  public void slop_is_2() throws IOException {
    // When slop is 2, the terms are matched exactly in the order specified.
    // 'open' is used to match prefix of the next term.
    String query = "SELECT Tags from %s where match_phrase_prefix(Tags, 'gas ta', slop=2)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("taste gas"));
  }

  @Test
  public void slop_is_3() throws IOException {
    // When slop is 3, results will include phrases where the query terms are transposed.
    String query = "SELECT Tags from %s where match_phrase_prefix(Tags, 'gas ta', slop=3)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_BEER));
    verifyDataRows(result, rows("taste draught gas"), rows("taste gas"));
  }
}
