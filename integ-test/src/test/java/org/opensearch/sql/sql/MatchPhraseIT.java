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
  public void test_match_phrase_with_slop() throws IOException {
    String query = "SELECT phrase FROM %s WHERE match_phrase(phrase, 'brown fox', slop = 2)";
    JSONObject result = executeJdbcRequest(String.format(query, TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"), rows("fox brown"));
  }
}
