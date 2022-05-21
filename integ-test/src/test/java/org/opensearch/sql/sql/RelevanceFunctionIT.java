/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class RelevanceFunctionIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void match_in_where() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_ACCOUNT + " WHERE match(lastname, 'Bates')");
    verifySchema(result, schema("firstname", "text"));
    verifyDataRows(result, rows("Nanette"));
  }

  @Test
  public void match_in_having() throws IOException {
    JSONObject result = executeJdbcRequest("SELECT lastname FROM " + TEST_INDEX_ACCOUNT + " HAVING match(firstname, 'Nanette')");
    verifySchema(result, schema("lastname", "text"));
    verifyDataRows(result, rows("Bates"));
  }

}
