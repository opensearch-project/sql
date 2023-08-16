/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MatchPhraseIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.PHRASE);
  }

  @Test
  public void test_match_phrase_function() throws IOException {
    String query = "SELECT phrase FROM %s WHERE match_phrase(phrase, 'quick fox')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void test_matchphrase_legacy_function() throws IOException {
    String query = "SELECT phrase FROM %s WHERE matchphrase(phrase, 'quick fox')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void test_matchphrasequery_legacy_function() throws IOException {
    String query = "SELECT phrase FROM %s WHERE matchphrasequery(phrase, 'quick fox')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void test_match_phrase_with_slop() throws IOException {
    String query = "SELECT phrase FROM %s WHERE match_phrase(phrase, 'brown fox', slop = 2)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"), rows("fox brown"));
  }

  @Test
  public void test_aliases_for_match_phrase_returns_same_result() throws IOException {
    String query1 = "SELECT phrase FROM %s WHERE matchphrase(phrase, 'quick fox')";
    String query2 = "SELECT phrase FROM %s WHERE match_phrase(phrase, 'quick fox')";
    String query3 = "SELECT phrase FROM %s WHERE matchphrasequery(phrase, 'quick fox')";
    JSONObject result1 = executeJdbcRequest(String.format(query1, TEST_INDEX_PHRASE));
    JSONObject result2 = executeJdbcRequest(String.format(query2, TEST_INDEX_PHRASE));
    JSONObject result3 = executeJdbcRequest(String.format(query3, TEST_INDEX_PHRASE));
    assertTrue(result1.similar(result2));
    assertTrue(result1.similar(result3));
  }

  @Test
  public void match_phrase_alternate_syntax() throws IOException {
    String query = "SELECT phrase FROM %s WHERE phrase = match_phrase('quick fox')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void matchphrase_alternate_syntax() throws IOException {
    String query = "SELECT phrase FROM %s WHERE phrase = matchphrase('quick fox')";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void match_phrase_alternate_syntaxes_return_the_same_results() throws IOException {
    String query1 = "SELECT phrase FROM %s WHERE matchphrase(phrase, 'quick fox')";
    String query2 = "SELECT phrase FROM %s WHERE phrase = matchphrase('quick fox')";
    String query3 = "SELECT phrase FROM %s WHERE phrase = match_phrase('quick fox')";
    JSONObject result1 = executeJdbcRequest(String.format(query1, TEST_INDEX_PHRASE));
    JSONObject result2 = executeJdbcRequest(String.format(query2, TEST_INDEX_PHRASE));
    JSONObject result3 = executeJdbcRequest(String.format(query3, TEST_INDEX_PHRASE));
    assertTrue(result1.similar(result2));
    assertTrue(result1.similar(result3));
  }
}
