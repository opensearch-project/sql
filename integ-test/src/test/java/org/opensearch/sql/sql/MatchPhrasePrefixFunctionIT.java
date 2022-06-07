/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MatchPhrasePrefixFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
  }

  @Test
  public void match_phrase_prefix_required_parameters() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT lastname FROM " + TEST_INDEX_BANK
        + " WHERE match_phrase_prefix(address, 'Quentin Str')");
    verifyDataRows(result, rows("Mcpherson"));
  }
}
