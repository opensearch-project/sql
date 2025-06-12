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
import org.junit.Test;

public class MatchBoolPrefixIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.PHRASE);
  }

  @Test
  public void valid_query_match_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match_bool_prefix(phrase, 'qui') | fields phrase",
                TEST_INDEX_PHRASE));

    verifyDataRows(result, rows("quick fox"), rows("quick fox here"));
  }

  @Test
  public void optional_parameter_match_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match_bool_prefix(phrase, '2 tes', minimum_should_match=1,"
                    + " fuzziness=2) | fields phrase",
                TEST_INDEX_PHRASE));

    verifyDataRows(result, rows("my test"), rows("my test 2"));
  }

  @Test
  public void no_matches_test() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where match_bool_prefix(phrase, 'rice') | fields phrase",
                TEST_INDEX_PHRASE));

    assertEquals(0, result.getInt("total"));
  }
}
