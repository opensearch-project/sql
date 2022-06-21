/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class MatchPhrasePrefixWhereCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
  }
  @Test
  public void match_phrase_prefix_required_parameters() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK
        + "| where match_phrase_prefix(address, 'Quentin Str') | fields lastname");
    verifyDataRows(result, rows("Mcpherson"));
  }

  @Test
  public void match_phrase_prefix_all_parameters() throws IOException {
    JSONObject result = executeQuery("source=" + TEST_INDEX_BANK
        + "| where match_phrase_prefix(address, 'Quentin Str', "
        + "boost = 1.0, zero_terms_query='ALL',max_expansions = 3, analyzer='standard') "
        + "| fields lastname");
    verifyDataRows(result, rows("Mcpherson"));
  }
}
