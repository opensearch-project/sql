/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class MatchPhraseIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
  }

  @Test
  public void test_match_phrase_function() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match_phrase(phrase, 'quick fox') | fields phrase",
                TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void test_match_phrase_with_slop() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match_phrase(phrase, 'brown fox', slop = 2) | fields phrase",
                TEST_INDEX_PHRASE));
    verifyDataRows(result, rows("brown fox"), rows("fox brown"));
  }
}
